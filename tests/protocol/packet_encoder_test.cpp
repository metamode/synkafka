
#include "gtest/gtest.h"

#include "packet.h"

#include "slice.h"
#include "buffer.h"

using namespace synkafka;

TEST(Protocol, PacketEncoderPrimatives) 
{
	slice expected("\x00\x00\x00\x2b" 						// i32 Length prefix of whole packet (43)
				   "\x0c" 									// i8 = 12
				   "\x0b\xcd" 								// i16 = 3021
				   "\x3a\xde\x68\xb1" 						// i32 = 987654321
				   "\x01\xb6\x9b\x4b\xad\x59\xd6\x7d" 		// i64 = 123456789132465789
				   "\x00\x0b"								// String i16 length prefix (11)
				   "Hello World"
				   "\x00\x00\x00\x0b"						// Bytes i32 length prefix (11)
				   "Hello World"
				  ,47);

	PacketEncoder pe(128);

	int8_t i8 = 12;
	pe.io(i8);

	int16_t i16 = 3021;
	pe.io(i16);

	int32_t i32 = 987654321;
	pe.io(i32);

	int64_t i64 = 123456789132465789;
	pe.io(i64);

	std::string str("Hello World");
	pe.io(str);

	pe.io_bytes(str, COMP_None);

	ASSERT_TRUE(pe.ok());

	auto encoded = pe.get_as_slice(true);
	ASSERT_EQ(expected.size(), encoded.size());
	ASSERT_EQ(0, expected.compare(encoded))
		<< "Expected: <" << expected.hex() << "> ("<< expected.size() << ")\n"
		<< "Got:      <" << encoded.hex() << "> ("<< encoded.size() << ")";
}

TEST(Protocol, PacketEncodedCRC)
{
	std::string test_data("Hello World");

	// We will write a crc32 field then the string field and expect crc to be correct
	slice expected("\x5f\x76\xe2\x36" 	// CRC32
				   "\x00\x0b"			// String i16 length prefix (11)
				   "Hello World"
				  ,17);

	PacketEncoder pe(64);

	auto crc = pe.start_crc();
	pe.io(test_data);
	pe.end_crc(crc);

	ASSERT_TRUE(pe.ok());

	auto encoded = pe.get_as_slice(false);
	ASSERT_EQ(expected.size(), encoded.size());
	ASSERT_EQ(0, expected.compare(encoded))
		<< "Expected: <" << expected.hex() << "> ("<< expected.size() << ")\n"
		<< "Got:      <" << encoded.hex() << "> ("<< encoded.size() << ")";

}

TEST(Protocol, PacketEncoderLengthField)
{
	std::string test_data("Hello World");

	// We will write a length field then the string field and expect length to be correct
	slice expected("\x00\x00\x00\x0d" 	// Length (13)
				   "\x00\x0b"			// String i16 length prefix (11)
				   "Hello World"
				  ,17);

	PacketEncoder pe(64);

	auto len_field = pe.start_length();
	pe.io(test_data);
	pe.end_length(len_field);

	ASSERT_TRUE(pe.ok());

	auto encoded = pe.get_as_slice(false);
	ASSERT_EQ(expected.size(), encoded.size());
	ASSERT_EQ(0, expected.compare(encoded))
		<< "Expected: <" << expected.hex() << "> ("<< expected.size() << ")\n"
		<< "Got:      <" << encoded.hex() << "> ("<< encoded.size() << ")";

}

TEST(Protocol, PacketEncoderGZIP)
{
	std::string test_data("Hello World Hello World Hello World "
				   "Hello World Hello World Hello World "
				   "Hello World Hello World Hello World "
				   "Hello World Hello World Hello World");

	slice expected("\x00\x00\x00\x23" // i32 length prefix (35 bytes)
				   "\x1f\x8b\x08\x00\x00\x00\x00\x00\x00\x03\xf3\x48\xcd\xc9\xc9\x57\x08\xcf\x2f\xca\x49\x51\xf0\x18\x78\x36\x00\x03\x2c\xa3\xac\x8f\x00\x00\x00"
				  ,39);

	PacketEncoder pe(64);

	pe.io_bytes(test_data, COMP_GZIP);

	ASSERT_TRUE(pe.ok());

	auto encoded = pe.get_as_slice(false);
	ASSERT_EQ(expected.size(), encoded.size());
	ASSERT_EQ(0, expected.compare(encoded))
		<< "Expected: <" << expected.hex() << "> ("<< expected.size() << ")\n"
		<< "Got:      <" << encoded.hex() << "> ("<< encoded.size() << ")";

}

TEST(Protocol, PacketEncoderSnappy)
{
	std::string test_data("Hello World Hello World Hello World "
				   "Hello World Hello World Hello World "
				   "Hello World Hello World Hello World "
				   "Hello World Hello World Hello World");

	slice expected("\x00\x00\x00\x17" // i32 length prefix (35 bytes)
				   "\x8f\x01\x2c\x48\x65\x6c\x6c\x6f\x20\x57\x6f\x72\x6c\x64\x20\xfe\x0c\x00\xee\x0c\x00\x0d\x0c"
				  ,27);

	PacketEncoder pe(64);

	pe.io_bytes(test_data, COMP_Snappy);

	ASSERT_EQ(true, pe.ok());

	auto encoded = pe.get_as_slice(false);
	ASSERT_EQ(expected.size(), encoded.size());
	ASSERT_EQ(0, expected.compare(encoded))
		<< "Expected: <" << expected.hex() << "> ("<< expected.size() << ")\n"
		<< "Got:      <" << encoded.hex() << "> ("<< encoded.size() << ")";

}

TEST(Protocol, PacketEncoderBufferResize)
{
	std::string test_data("This string is longer than the encoder buffer we are writing it too.");
	slice expected("\x00\x44This string is longer than the encoder buffer we are writing it too.", 70);

	PacketEncoder pe(1); // Small buffer

	pe.io(test_data);

	ASSERT_TRUE(pe.ok());

	auto encoded = pe.get_as_slice(false);
	ASSERT_EQ(expected.size(), encoded.size());
	ASSERT_EQ(0, expected.compare(encoded))
		<< "Expected: <" << expected.hex() << "> ("<< expected.size() << ")\n"
		<< "Got:      <" << encoded.hex() << "> ("<< encoded.size() << ")";

}