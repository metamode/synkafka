#pragma once

#include <string>

#include "utils/buffer.h"
#include "utils/slice.h"

namespace synkafka {


class LengthField
{
public:
	LengthField(PacketEncoder* pe, size_t offset) : pe_(pe), offset_(offset) {}

	size_t getReservedLength() const { return sizeof(int32_t); };
	void end();
	bool check();

protected:
	PacketEncoder* pe_;
	size_t offset_;
};

class CRCField
{
public:
	CRCField(PacketEncoder* pe, size_t offset) : pe_(pe), offset_(offset) {}

	size_t getReservedLength() const { return sizeof(int32_t); };
	void end();
	bool check();

protected:
	PacketEncoder* pe_;
	size_t offset_;
};

/**
 * Basic primative for writing low level protocol encodings
 */
class PacketEncoder
{
public:
	typedef enum {ERR_NONE, ERR_MEM, ERR_COMPRESS_FAIL} err;

	explicit PacketEncoder(size_t initialBufferSize);
	explicit PacketEncoder(buffer_t&& buffer);

	// Methods to write primative types
	void push(int8_t value);
	void push(int16_t value);
	void push(int32_t value);
	void push(int64_t value);
	void push(const slice& value);
	void pushBytes(const slice& value);

	void pushCompressedBytes(const slice& value, CompressionType ctype);

	// Special helpers to enable writing fields that rely on later data to be correct
	LengthField pushLengthField();
	CRCField 	pushCRCField();

	// Returned slice is only valid while PacketEncoder remains valid and no
	// other non-const methods are called on it.
	const slice& getAsSlice() const;
	const slice& getSliceSince(size_t startOffset) const ;

	size_t getCursor() const { return cursor_; }

private:
	buffer_t buff_;
	size_t cursor_;

};

class PacketDecoder
{
public:
	typedef enum {ERR_NONE, ERR_MEM, ERR_DECOMPRESS_FAIL, ERR_CORRUPT} err;


private:
	buffer_t buff_;
	size_t cursor_;
};

}