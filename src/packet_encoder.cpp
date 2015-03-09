
#include <cassert>
#include <cstring> // memcpy
#include <limits>

#include "crc32.h"
#include "portable_endian.h"
#include <snappy.h>
#include <zlib.h>

#include "packet.h"

namespace synkafka {

PacketEncoder::PacketEncoder(size_t buffer_size)
	: buff_(buffer_size + sizeof(int32_t))
{
	// Reserve Space for length prefix
	cursor_ = sizeof(int32_t);
}

void PacketEncoder::io(int8_t& value)
{
	if (!ok()) return; 
	buff_.reserve(buff_.size() + sizeof(int8_t));
	buff_[cursor_] = static_cast<uint8_t>(value);
	cursor_ += sizeof(int8_t);
}

void PacketEncoder::io(int16_t& value)
{
	if (!ok()) return; 
	buff_.reserve(buff_.size() + sizeof(int16_t));
	auto netVal = htobe16(reinterpret_cast<uint16_t&>(value));
	*reinterpret_cast<uint16_t*>(&buff_[cursor_]) = netVal;
	cursor_ += sizeof(int16_t);
}

void PacketEncoder::io(int32_t& value)
{
	if (!ok()) return; 
	buff_.reserve(buff_.size() + sizeof(int32_t));
	auto netVal = htobe32(reinterpret_cast<uint32_t&>(value));
	*reinterpret_cast<uint32_t*>(&buff_[cursor_]) = netVal;
	cursor_ += sizeof(int32_t);
}

void PacketEncoder::io(int64_t& value)
{
	if (!ok()) return; 
	buff_.reserve(buff_.size() + sizeof(int64_t));
	auto netVal = htobe64(reinterpret_cast<uint64_t&>(value));
	*reinterpret_cast<uint64_t*>(&buff_[cursor_]) = netVal;
	cursor_ += sizeof(int64_t);
}

void PacketEncoder::io(std::string& value)
{
	if (!ok()) return;
	if (value.size() > std::numeric_limits<int16_t>::max()) {
		set_err(ERR_INVALID_VALUE)
			<< "String value was longer than int16 prefix allows";
		return;
	}
	// Optimisation, reserve whole length now rather than risk two reallocations
	buff_.reserve(buff_.size() + sizeof(int16_t) + value.size());

	// Write i16 length prefix, note that this moves cursor
	int16_t len = value.size();
	io(len);

	// Check we are still OK
	if (!ok()) return;

	// Write value
	std::memcpy(&buff_[cursor_], value.data(), value.size());
	cursor_ += value.size();	
}

void PacketEncoder::io_bytes(std::string& value, CompressionType ctype)
{
	if (!ok()) return;

	switch (ctype)
	{
	case COMP_None:
		{
			if (value.size() > std::numeric_limits<int32_t>::max()) {
				set_err(ERR_INVALID_VALUE)
				 	<< "Bytes value was longer than int32 prefix allows";
				return;
			}
			// Optimisation, reserve whole length now rather than risk two reallocations
			buff_.reserve(buff_.size() + sizeof(int32_t) + value.size());

			// Write i32 length prefix, note that this moves cursor
			int32_t len = value.size();
			io(len);

			// Check we are still OK
			if (!ok()) return;

			// Write value
			std::memcpy(&buff_[cursor_], value.data(), value.size());
			cursor_ += value.size();
		}
		break;

	case COMP_GZIP:
		{
			z_stream strm;

			// Initialize gzip compression
			memset(&strm, 0, sizeof(strm));
			auto r = deflateInit2(&strm, Z_DEFAULT_COMPRESSION,
					 Z_DEFLATED, 15+16, // 15+16 forces zlib to add the gzip header and trailer
					 8, Z_DEFAULT_STRATEGY);

			if (r != Z_OK) {
				set_err(ERR_COMPRESS_FAIL)
					<< "Failed to initialize GZIP for compression";
				return;
			}

			// Calculate maximum compressed size and reserve space in output buffer
			auto comp_size_max = deflateBound(&strm, value.size());
			buff_.reserve(buff_.size() + sizeof(int32_t) + comp_size_max);

			// Use a length field internally to reserve space for length and write it
			auto length_prefix = start_length();

			// Cursor is update to after length field...
			strm.next_out = static_cast<Bytef*>(&buff_[cursor_]);
			strm.avail_out = buff_.capacity() - cursor_;

			strm.next_in = const_cast<Bytef*>(reinterpret_cast<const unsigned char *>(value.data()));
			strm.avail_in = value.size();

			// Compress - we allocated enough space so we should never need multiple
			// calls to deflate
			if ((r = deflate(&strm, Z_FINISH) != Z_STREAM_END)) {
				deflateEnd(&strm);
				set_err(ERR_COMPRESS_FAIL) 
					<< "Failed GZIP compression";
				return;
			}

			// Move cursor to end of comopressed bytes
			cursor_ += strm.total_out;

			// Deinitialize compression
			deflateEnd(&strm);

			// Update length prefix
			end_length(length_prefix);
		}	
		break;

	case COMP_Snappy:
		{
			auto comp_size_max = snappy::MaxCompressedLength(value.size());

			// Reserve space for whole thing including prefix first
			buff_.reserve(buff_.size() + sizeof(int32_t) + comp_size_max);

			auto length_prefix = start_length();

			size_t output_len = 0;
			snappy::RawCompress(reinterpret_cast<const char *>(value.data())
							   ,value.size()
							   ,reinterpret_cast<char *>(&buff_[cursor_])
							   ,&output_len);

			if (output_len < 1) {
				set_err(ERR_COMPRESS_FAIL)
					<< "Snappy compression failed...";
				return;
			}

			// Move cursor to end of comopressed bytes
			cursor_ += output_len;

			// Update length prefix
			end_length(length_prefix);
		}
		break;
	}
}

size_t PacketEncoder::start_crc()
{
	// Bail earlyso we don't continue to update state...
	if (!ok()) return cursor_;

	// Reserve room for field
	buff_.reserve(buff_.size() + sizeof(int32_t));
	auto start = cursor_;
	cursor_ += sizeof(int32_t);

	return start;
}

void   PacketEncoder::end_crc(size_t field_offset)
{
	if (!ok()) return;

	assert(buff_.size() > field_offset + sizeof(int32_t));
	assert(cursor_ > field_offset + sizeof(int32_t));

	// Calculate CRC32 on the data in the buffer immediately after field_offset
	CRC32 crc;
	crc.add(&buff_[field_offset + sizeof(int32_t)], cursor_ - field_offset - sizeof(int32_t));
	int32_t crc32 = static_cast<int32_t>(crc.get());

	auto current_cursor = cursor_;

	seek(field_offset);
	io(crc32);
	seek(current_cursor);
}

size_t PacketEncoder::start_length()
{
	// Bail earlyso we don't continue to update state...
	if (!ok()) return cursor_;

	// Reserve room for field
	buff_.reserve(buff_.size() + sizeof(int32_t));
	auto start = cursor_;
	cursor_ += sizeof(int32_t);

	return start;
}

void   PacketEncoder::end_length(size_t field_offset)
{
	if (!ok()) return; 

	assert(buff_.size() > field_offset + sizeof(int32_t));
	assert(cursor_ > field_offset + sizeof(int32_t));

	int32_t length = static_cast<int32_t>(cursor_ - field_offset - sizeof(int32_t));

	auto current_cursor = cursor_;

	seek(field_offset);
	io(length);
	seek(current_cursor);
}

const slice PacketEncoder::get_as_slice(bool with_length_prefix)
{
	if (!with_length_prefix) {
		// Ignore length prefix
		return slice(&buff_[sizeof(int32_t)], cursor_ - sizeof(int32_t));
	}

	// Calculate length prefix from cursor and write it.
	auto current_cursor = cursor_;
	seek(0);
	// Write size at head (minus this length prefix itself)
	int32_t len = current_cursor - sizeof(int32_t);
	io(len);
	// Reset cursor
	seek(current_cursor);

	return slice(&buff_[0], cursor_);
}


}