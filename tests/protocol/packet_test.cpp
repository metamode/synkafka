
#include "gtest/gtest.h"

#include "protocol/packet.h"

#include "utils/slice.h"
#include "utils/buffer.h"

using namespace synkafka;

TEST(Protocol, PacketEncoderPrimatives) 
{
	slice expected("", 0);

	PacketEncoder pe(128);

	int8_t i8 = 12;
	pe.push(i8);

	int8_t i16 = 3021;
	pe.push(i16);

	int8_t i32 = 987654321;
	pe.push(i32);

	int8_t i64 = 123456789132465789;
	pe.push(i64);

	slice str("Hello World");
	pe.push(str);

	pe.pushBytes(str);

	ASSERT_EQ(true, pe.ok());

	auto encoded = pe.getAsSlice();
	ASSERT_EQ(expected.size(), encoded.size());
	ASSERT_EQ(0, expected.compare(encoded))
		<< "Expected: <" << expected.hex() << "> ("<< expected.size() << ")\n"
		<< "Got:      <" << encoded.hex() << "> ("<< encoded.size() << ")";
}