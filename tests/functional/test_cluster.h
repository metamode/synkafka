#pragma once

#include <cstdlib>
#include <string>

inline int get_env_int(const std::string& name)
{
	std::string env_name("SYNKAFKA_FUNC_");
	env_name += name;

	auto val = getenv(env_name.c_str());
	if (val == nullptr) {
		return 0;
	}

	return std::atoi(val);
}

inline std::string get_env_string(const std::string& name)
{
	std::string env_name("SYNKAFKA_FUNC_");
	env_name += name;

	auto val = getenv(env_name.c_str());
	if (val == nullptr) {
		return "";
	}

	return std::string(val);
}