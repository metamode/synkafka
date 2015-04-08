
#include <cassert>
#include <cstring> // memcpy
#include <limits>
#include <iostream>

#include "crc32.h"
#include "portable_endian.h"
#include <snappy.h>
#include <zlib.h>

#include "packet.h"

namespace synkafka {

PacketEncoder::PacketEncoder(size_t buffer_size)
	: PacketCodec()
	, buff_(buffer_size + sizeof(int32_t))
{
	// Reserve Space for length prefix
	update_size_after_write(sizeof(int32_t));
}

void PacketEncoder::io(int8_t& value)
{
	if (!ok()) return; 
	ensure_space_for(sizeof(int8_t));
	buff_[cursor_] = static_cast<uint8_t>(value);
	update_size_after_write(sizeof(int8_t));
}

void PacketEncoder::io(int16_t& value)
{
	if (!ok()) return; 
	ensure_space_for(sizeof(int16_t));
	auto netVal = htobe16(reinterpret_cast<uint16_t&>(value));
	*reinterpret_cast<uint16_t*>(&buff_[0] + cursor_) = netVal;
	update_size_after_write(sizeof(int16_t));
}

void PacketEncoder::io(int32_t& value)
{
	if (!ok()) return; 
	ensure_space_for(sizeof(int32_t));
	auto netVal = htobe32(reinterpret_cast<uint32_t&>(value));
	*reinterpret_cast<uint32_t*>(&buff_[0] + cursor_) = netVal;
	update_size_after_write(sizeof(int32_t));
}

void PacketEncoder::io(int64_t& value)
{
	if (!ok()) return; 
	ensure_space_for(sizeof(int64_t));
	auto netVal = htobe64(reinterpret_cast<uint64_t&>(value));
	*reinterpret_cast<uint64_t*>(&buff_[0] + cursor_) = netVal;
	update_size_after_write(sizeof(int64_t));
}

void PacketEncoder::io(std::error_code& value)
{
	int16_t err = static_cast<int16_t>(value.value());
	io(err);
}

void PacketEncoder::io(std::string& value)
{
	slice v(value);
	io(v);
}

void PacketEncoder::io(slice& value)
{
	if (!ok()) return;
	if (value.size() > std::numeric_limits<int16_t>::max()) {
		set_err(ERR_INVALID_VALUE)
			<< "String value was longer than int16 prefix allows";
		return;
	}
	// Optimisation, reserve whole length now rather than risk two reallocations
	ensure_space_for(sizeof(int16_t) + value.size());

	// Write i16 length prefix, note that this moves cursor
	int16_t len = value.size();

	// Empty strings encode as "null" with -1 length prefix
	if (len == 0) {
		len = -1;
	}

	io(len);

	// Check we are still OK
	if (!ok()) return;

	if (len < 0) {
		return;
	}

	// Write value
	std::memcpy(&buff_[0] + cursor_, value.data(), value.size());
	update_size_after_write(value.size());	
}

void PacketEncoder::io_bytes(std::string& value, CompressionType ctype)
{
	slice v(value);
	io_bytes(v, ctype);
}

void PacketEncoder::io_bytes(slice& value, CompressionType ctype)
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
			ensure_space_for(sizeof(int32_t) + value.size());

			// Write i32 length prefix, note that this moves cursor
			int32_t len = value.size();

			// Empty bytes encode as "null" with -1 length prefix
			if (len == 0) {
				len = -1;
			}

			io(len);

			// Check we are still OK
			if (!ok()) return;
			
			if (len < 0) {
				return;
			}

			// Write value
			std::memcpy(&buff_[0] + cursor_, value.data(), value.size());
			update_size_after_write(value.size());
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
			ensure_space_for(sizeof(int32_t) + comp_size_max);

			// Use a length field internally to reserve space for length and write it
			auto length_prefix = start_length();

			// Cursor is update to after length field...
			strm.next_out = static_cast<Bytef*>(&buff_[0] + cursor_);
			strm.avail_out = buff_.size() - cursor_;

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

			// Move cursor to end of compressed bytes
			update_size_after_write(strm.total_out);

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
			ensure_space_for(sizeof(int32_t) + comp_size_max);

			auto length_prefix = start_length();

			size_t output_len = 0;
			snappy::RawCompress(reinterpret_cast<const char *>(value.data())
							   ,value.size()
							   ,reinterpret_cast<char *>(&buff_[0] + cursor_)
							   ,&output_len);

			if (output_len < 1) {
				set_err(ERR_COMPRESS_FAIL)
					<< "Snappy compression failed...";
				return;
			}

			// Move cursor to end of compressed bytes
			update_size_after_write(output_len);

			// Update length prefix
			end_length(length_prefix);
		}
		break;
	}
}

size_t PacketEncoder::start_crc()
{
	// Bail early so we don't continue to update state...
	if (!ok()) return cursor_;

	// Reserve room for field
	ensure_space_for(sizeof(int32_t));
	auto start = cursor_;
	update_size_after_write(sizeof(int32_t));

	return start;
}

void   PacketEncoder::end_crc(size_t field_offset)
{
	if (!ok()) return;

	assert(size_ > field_offset + sizeof(int32_t));
	assert(cursor_ > field_offset + sizeof(int32_t));

	// Calculate CRC32 on the data in the buffer immediately after field_offset
	CRC32 crc;
	crc.add(&buff_[0] + field_offset + sizeof(int32_t), cursor_ - field_offset - sizeof(int32_t));
	int32_t crc32 = static_cast<int32_t>(crc.get());

	auto current_cursor = cursor_;

	seek(field_offset);
	io(crc32);
	seek(current_cursor);
}

size_t PacketEncoder::start_length()
{
	// Bail early so we don't continue to update state...
	if (!ok()) return cursor_;

	// Reserve room for field
	ensure_space_for(sizeof(int32_t));
	auto start = cursor_;
	update_size_after_write(sizeof(int32_t));

	return start;
}

void   PacketEncoder::end_length(size_t field_offset)
{
	if (!ok()) return; 

	assert(size_ >= field_offset + sizeof(int32_t));
	assert(cursor_ >= field_offset + sizeof(int32_t));

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
		return slice(&buff_[0] + sizeof(int32_t), size_ - sizeof(int32_t));
	}

	update_length(0);

	return slice(&buff_[0], size_);
}

const slice PacketEncoder::get_as_buffer_sequence_head(size_t rest_of_buffer)
{
	update_length(rest_of_buffer);

	return slice(&buff_[0], size_);
}

void PacketEncoder::update_length(size_t extra_length)
{
	// Write size at head (minus this length prefix itself, but adding any additional length needed)
	auto current_cursor = cursor_;
	seek(0);
	int32_t len = size_ - sizeof(int32_t) + extra_length;
	io(len);
	// Reset cursor
	seek(current_cursor);
}

void PacketEncoder::ensure_space_for(size_t len)
{
	// We can't just use reserve() since we write direct to underlying storage
	// which means we might write past end of size() and into capacity() which 
	// causes undefined behaiour or ugly checks everywhere.
	// See 
	if (buff_.size() < (size_ + len)) {
		// Not enough room, increase size of buffer.
		// We find smallest power of 2 which is large enough for len and increase that many times
		size_t power = 1;
		while ((buff_.size() << power) < (size_ + len)) {
			++power;
		}
		buff_.resize(buff_.size() << power);
	}
}

}