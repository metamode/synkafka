
#include "gtest/gtest.h"

#include "packet.h"

#include "slice.h"
#include "buffer.h"

using namespace synkafka;

TEST(Protocol, PacketDecoderPrimatives) 
{
	auto input = buffer_from_string("\x0c" 									// i8 = 12
									"\x0b\xcd" 								// i16 = 3021
									"\x3a\xde\x68\xb1" 						// i32 = 987654321
									"\x01\xb6\x9b\x4b\xad\x59\xd6\x7d" 		// i64 = 123456789132465789
									"\x00\x0b"								// String i16 length prefix (11)
									"Hello World"
									"\x00\x00\x00\x0b"						// Bytes i32 length prefix (11)
									"Hello World"
				   					"\xFF\xFF"								// Null string prefix
				   					"\xFF\xFF\xFF\xFF"						// Null Bytes prefix
								   ,49);

	PacketDecoder pd(input);

	int8_t i8 = 0;
	pd.io(i8);
	ASSERT_TRUE(pd.ok());
	EXPECT_EQ(12, i8);

	int16_t i16 = 0;
	pd.io(i16);
	ASSERT_TRUE(pd.ok());
	EXPECT_EQ(3021, i16);

	int32_t i32 = 0;
	pd.io(i32);
	ASSERT_TRUE(pd.ok());
	EXPECT_EQ(987654321, i32);

	int64_t i64 = 0;
	pd.io(i64);
	ASSERT_TRUE(pd.ok());
	EXPECT_EQ(123456789132465789, i64);

	std::string str;
	pd.io(str);
	ASSERT_TRUE(pd.ok());
	EXPECT_EQ("Hello World", str);

	pd.io_bytes(str, COMP_None);
	ASSERT_TRUE(pd.ok());
	EXPECT_EQ("Hello World", str);	

	pd.io(str);
	ASSERT_TRUE(pd.ok());
	EXPECT_EQ("", str);

	pd.io_bytes(str, COMP_None);
	ASSERT_TRUE(pd.ok());
	EXPECT_EQ("", str);	
}

TEST(Protocol, PacketDecoderCRC)
{
	auto input = buffer_from_string("\x5f\x76\xe2\x36" 	// CRC32
				                    "\x00\x0b"			// String i16 length prefix (11)
				                    "Hello World"
				                   ,17);

	{
		PacketDecoder pd(input);

		auto crc = pd.start_crc();

		std::string str;
		pd.io(str);
		ASSERT_TRUE(pd.ok()) << pd.err_str();
		EXPECT_EQ("Hello World", str);

		// Perform CRC check
		pd.end_crc(crc);
		ASSERT_TRUE(pd.ok()) << pd.err_str();
	}
	
	// Also test it catches corruption
	input->at(6) = 'F'; // Change "Hello World" to "Fellow World";
	
	{
		PacketDecoder pd(input);

		auto crc = pd.start_crc();

		std::string str;
		pd.io(str);
		ASSERT_TRUE(pd.ok()) << pd.err_str();
		EXPECT_EQ("Fello World", str);

		// Perform CRC check
		pd.end_crc(crc);
		ASSERT_FALSE(pd.ok()) << pd.err_str();
		ASSERT_EQ(PacketCodec::ERR_CHECKSUM_FAIL, pd.err());
	}
}

TEST(Protocol, PacketDecoderLengthField)
{
	auto input = buffer_from_string("\x00\x00\x00\x0d" 	// Length (13)
				                    "\x00\x0b"			// String i16 length prefix (11)
				                    "Hello World"
				                   ,17);

	PacketDecoder pd(input);

	auto len_field = pd.start_length();

	std::string str;
	pd.io(str);
	ASSERT_TRUE(pd.ok()) << pd.err_str();
	EXPECT_EQ("Hello World", str);

	pd.end_length(len_field);
	ASSERT_TRUE(pd.ok()) << pd.err_str();
}

TEST(Protocol, PacketDecoderGZIP)
{
	std::string expected("Hello World Hello World Hello World "
				   		 "Hello World Hello World Hello World "
				   		 "Hello World Hello World Hello World "
				   		 "Hello World Hello World Hello World");

	auto input = buffer_from_string("\x00\x00\x00\x23" // i32 length prefix (35 bytes)
				  				    "\x1f\x8b\x08\x00\x00\x00\x00\x00\x00\x03\xf3\x48\xcd\xc9\xc9\x57\x08\xcf\x2f\xca\x49\x51\xf0\x18\x78\x36\x00\x03\x2c\xa3\xac\x8f\x00\x00\x00"
				  				   ,39);

	PacketDecoder pd(input);

	std::string str;
	pd.io_bytes(str, COMP_GZIP);
	ASSERT_TRUE(pd.ok()) << pd.err_str();
	ASSERT_EQ(expected, str)
		<< "Expected: <" << slice(expected).hex() << "> ("<< expected.size() << ")\n"
		<< "Got:      <" << slice(str).hex() << "> ("<< str.size() << ")";

}

TEST(Protocol, PacketDecoderSnappy)
{
	std::string expected("Hello World Hello World Hello World "
				   		 "Hello World Hello World Hello World "
				   		 "Hello World Hello World Hello World "
				   		 "Hello World Hello World Hello World");

	auto input = buffer_from_string("\x00\x00\x00\x17" // i32 length prefix (35 bytes)
				  				    "\x8f\x01\x2c\x48\x65\x6c\x6c\x6f\x20\x57\x6f\x72\x6c\x64\x20\xfe\x0c\x00\xee\x0c\x00\x0d\x0c"
				  				   ,27);

	PacketDecoder pd(input);

	std::string str;
	pd.io_bytes(str, COMP_Snappy);
	ASSERT_TRUE(pd.ok()) << pd.err_str();
	ASSERT_EQ(expected, str)
		<< "Expected: <" << slice(expected).hex() << "> ("<< expected.size() << ")\n"
		<< "Got:      <" << slice(str).hex() << "> ("<< str.size() << ")";
}
