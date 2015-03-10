
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

PacketDecoder::PacketDecoder(shared_buffer_t buffer)
	: buff_(buffer)
{}

void PacketDecoder::io(int8_t& value)
{
	if (!can_read(sizeof(int8_t))) return;

	value = static_cast<int8_t>(buff_->at(cursor_));
	cursor_ += sizeof(int8_t);
}

void PacketDecoder::io(int16_t& value)
{
	if (!can_read(sizeof(int16_t))) return;

	auto netVal = reinterpret_cast<const uint16_t*>(&buff_->at(cursor_));
	value = static_cast<int16_t>(be16toh(*netVal));
	cursor_ += sizeof(int16_t);
}

void PacketDecoder::io(int32_t& value)
{
	if (!can_read(sizeof(int32_t))) return;

	auto netVal = reinterpret_cast<const uint32_t*>(&buff_->at(cursor_));
	value = static_cast<int32_t>(be32toh(*netVal));
	cursor_ += sizeof(int32_t);
}

void PacketDecoder::io(int64_t& value)
{
	if (!can_read(sizeof(int64_t))) return;

	auto netVal = reinterpret_cast<const uint64_t*>(&buff_->at(cursor_));
	value = static_cast<int64_t>(be64toh(*netVal));
	cursor_ += sizeof(int64_t);
}

void PacketDecoder::io(std::string& value)
{
	// Reat the length prefix
	int16_t len = 0;
	io(len);

	// -1 length used for "null" value
	if (ok() && len == -1) {
		value = "";
		return;
	}

	if (!can_read(len)) return;

	value = std::move(std::string(reinterpret_cast<char *>(&buff_->at(cursor_)), len));
	cursor_ += len;
}

void PacketDecoder::io_bytes(std::string& value, CompressionType ctype)
{
	// Reat the length prefix
	int32_t len = 0;
	io(len);

	// -1 length used for "null" value
	if (ok() && len == -1) {
		value = "";
		return;
	}

	if (!can_read(len)) return;

	switch (ctype)
	{
	case COMP_None:
		value = std::move(std::string(reinterpret_cast<char *>(&buff_->at(cursor_)), len));
		cursor_ += len;
		break;

	case COMP_GZIP:
	{
		const size_t CHUNK = 128 * 1024;

	    unsigned have;
	    z_stream strm;
	    unsigned char buff[CHUNK];

		// Initialize gzip compression
		memset(&strm, 0, sizeof(strm));
		auto r = inflateInit2(&strm, 15+32); // 15+32 is magic that makes us look for proepr GZIP header on just zlib one

		if (r != Z_OK) {
			set_err(ERR_COMPRESS_FAIL)
				<< "Failed to initialize GZIP for decompression";
			return;
		}

	    strm.avail_in = len;
	    strm.next_in = reinterpret_cast<Bytef*>(&buff_->at(cursor_));

	    /* run inflate() on input until output buffer not full */
	    do {
	        strm.avail_out = CHUNK;
	        strm.next_out = buff;
	        r = inflate(&strm, Z_NO_FLUSH);
	        assert(r != Z_STREAM_ERROR);  /* state not clobbered */
	        switch (r) {
	        case Z_NEED_DICT:
	        case Z_DATA_ERROR:
	        case Z_MEM_ERROR:
	            (void)inflateEnd(&strm);
				set_err(ERR_COMPRESS_FAIL)
					<< "GZIP inflate failed: " << strm.msg;
	            return;
	        }
	        have = CHUNK - strm.avail_out;

	        value.reserve(have);

	        std::copy(&buff[0], &buff[have], std::back_inserter(value));

	    } while (strm.avail_out == 0);

	    /* clean up and return */
	    (void)inflateEnd(&strm);

		// Move cursor to end of compressed bytes
		cursor_ += len;
	}	
		break;

	case COMP_Snappy:
	{
		if (!snappy::Uncompress(reinterpret_cast<char *>(&buff_->at(cursor_)), len, &value)) {
			set_err(ERR_COMPRESS_FAIL)
				<< "Failed Snappy Uncomopress";
		}

		// Move cursor to end of compressed bytes
		cursor_ += len;
	}
		break;
	}
}

size_t PacketDecoder::start_crc()
{
	auto start = cursor_;
	// Advance cursor past CRC
	cursor_ += sizeof(int32_t);
	return start;
}

void   PacketDecoder::end_crc(size_t field_offset)
{
	if (!ok()) return;

	assert(buff_->size() > field_offset + sizeof(int32_t));
	assert(cursor_ > field_offset + sizeof(int32_t));

	// Calculate CRC32 on the data in the buffer immediately after field_offset
	CRC32 crc;
	crc.add(&buff_->at(field_offset + sizeof(int32_t)), cursor_ - field_offset - sizeof(int32_t));
	int32_t calculated_crc32 = static_cast<int32_t>(crc.get());

	// No go back and read actual crc32 and check they match
	auto current_cursor = cursor_;
	int32_t given_crc32;

	seek(field_offset);
	io(given_crc32);
	seek(current_cursor);

	if (given_crc32 != calculated_crc32) {
		set_err(ERR_CHECKSUM_FAIL)
			<< "CRC32 did no match. Calculated: " << calculated_crc32 
			<< " expected " << given_crc32
			<< " at message starting at offset " << field_offset
			<< " with length " << cursor_ - field_offset;
	}
}

size_t PacketDecoder::start_length()
{
	auto start = cursor_;
	cursor_ += sizeof(int32_t);
	return start;
}

void   PacketDecoder::end_length(size_t field_offset)
{
	// No op. checking on read seems pointless. Few places it's needed
	// had to read and hack it in a different way.
}

bool PacketDecoder::can_read(size_t bytes)
{
	if (ok()) {
		auto remaining = buff_->size() - cursor_;
		if (bytes > remaining) {
			set_err(ERR_TRUNCATED)
				<< "Tried to read more bytes than we have available in buffer. "
				<< bytes << " requested " << remaining << " of " << buff_->size() << " remain";
			return false;
		}
		return true;
	}
	return false;
}


}