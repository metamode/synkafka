#pragma once

#include <deque>
#include <string>
#include <sstream>

#include "buffer.h"
#include "slice.h"
#include "constants.h"

namespace synkafka {

/**
 * Basic primative for reading and writing low level protocol primatives
 */
class PacketCodec
{
public:
	typedef enum {ERR_NONE, ERR_MEM, ERR_INVALID_VALUE, ERR_COMPRESS_FAIL, ERR_TRUNCATED, ERR_CHECKSUM_FAIL, ERR_LOGIC} err_t;

	// Methods to read/write primative types
	virtual void io(int8_t& value) = 0;
	virtual void io(int16_t& value) = 0;
	virtual void io(int32_t& value) = 0;
	virtual void io(int64_t& value) = 0;
	virtual void io(std::string& value) = 0;
	virtual void io_bytes(std::string& value, CompressionType ctype) = 0;

	// Special helpers to enable writing fields that rely on later data to be correct
	// Start methods reservce spac ein buffer for final field and return the byte offset 
	// into the buffer the field exists at.
	// end_* methods caclulate the value of the field of the bytes between the start and
	// current cursor and either write the value to buffer or check and raise error depending
	// on whether packet is reading or writing..
	virtual size_t start_crc() = 0;
	virtual void   end_crc(size_t field_offset) = 0;

	virtual size_t start_length() = 0;
	virtual void   end_length(size_t field_offset) = 0;

	virtual bool is_writer() const = 0;

	// Template member that proxies to externally defined codec methods for each specific protocol struct
	template<typename T>
	void io(T& type)
	{
		kafka_proto_io(*this, type);
	}

	size_t get_cursor() const { return cursor_; };
	// REQUIRES: offset < current buffer size
	void seek(size_t offset) { cursor_ = offset; };

	bool ok() const { return err_ == ERR_NONE; };
	err_t err() const { return err_; };
	std::string err_str() const { return err_stream_.str(); };

	std::stringstream& set_err(err_t error)
	{
		err_ = error;
		// Clear error message stream
		err_stream_.str("");
		err_stream_.clear();

		return err_stream_;
	}

protected:
	// Protect default constructor as this should only be used derived from
	PacketCodec() : err_(ERR_NONE), err_stream_(""), cursor_(0) {} 

	err_t				err_;
	std::stringstream 	err_stream_;
	size_t 				cursor_;
};

class PacketEncoder : public PacketCodec
{
public:
	explicit PacketEncoder(size_t buffer_size);

	void io(int8_t& value) override;
	void io(int16_t& value) override;
	void io(int32_t& value) override;
	void io(int64_t& value) override;
	void io(std::string& value) override;
	void io_bytes(std::string& value, CompressionType ctype) override;

	size_t start_crc() override;
	void   end_crc(size_t field_offset) override;

	size_t start_length() override;
	void   end_length(size_t field_offset) override;

	bool is_writer() const override { return true; };

	// Template member that proxies to externally defined codec methods for each specific protocol struct
	template<typename T>
	void io(T& type)
	{
		kafka_proto_io(*this, type);
	}

	const slice get_as_slice(bool with_length_prefix);

private:
	buffer_t 	buff_;

};

class PacketDecoder : public PacketCodec
{
public:
	explicit PacketDecoder(shared_buffer_t);

	void io(int8_t& value) override;
	void io(int16_t& value) override;
	void io(int32_t& value) override;
	void io(int64_t& value) override;

	// Slice returned points to shared buffer so it is only valid as long
	// as the PacketDecoder is around, if stronger guarantee is needed, copy it
	// or we could implement method to allow the caller to make also take a copy
	// of underlying shared buffer so it keeps ref alive even if Encoder goes away.
	void io(std::string& value) override;
	void io_bytes(std::string& value, CompressionType ctype) override;

	size_t start_crc() override;
	void   end_crc(size_t field_offset) override;

	size_t start_length() override;
	void   end_length(size_t field_offset) override;

	bool is_writer() const override { return false; };

	// Template member that proxies to externally defined codec methods for each specific protocol struct
	template<typename T>
	void io(T& type)
	{
		kafka_proto_io(*this, type);
	}

private:
	shared_buffer_t buff_;

	bool can_read(size_t bytes);
};


// Templates for common io methods for std::deque
// Since we can't partially specialise io member function templates directly
template<typename T>
void  kafka_proto_io(PacketCodec& p, std::deque<T>& type)
{
	if (p.is_writer()) {
		// Encode length as int32_t
		int32_t size = type.size();
		p.io(size);
		for (T& t : type) {
			p.io(t);
		}
	} else {
		// Decode length as int32_t
		int32_t size = 0;
		p.io(size);
		for (; size > 0; --size) {
			T element;
			p.io(element);
			if (p.ok()) {
				type.push_back(std::move(element));
			} else {
				return;
			}
		}
	}
}

}