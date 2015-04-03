#pragma once

#include <atomic>

#include "spdlog/spdlog.h"

namespace synkafka {

static auto log = spdlog::stdout_logger_mt("console");

static std::atomic<bool> init_done(false);

static inline void init_log() {
	if (init_done.load()) return;
	init_done = true;

	auto level = getenv("LOG_LEVEL");
	if (level == nullptr) {
		log->set_level(spdlog::level::warn);
	} else if (strcmp(level, "TRACE") == 0) {		
		log->set_level(spdlog::level::trace);
	} else if (strcmp(level, "DEBUG") == 0) {		
		log->set_level(spdlog::level::debug);
	} else if (strcmp(level, "INFO") == 0) {		
		log->set_level(spdlog::level::info);
	} else if (strcmp(level, "NOTICE") == 0) {		
		log->set_level(spdlog::level::notice);
	} else if (strcmp(level, "WARN") == 0) {		
		log->set_level(spdlog::level::warn);
	} else if (strcmp(level, "ERR") == 0) {		
		log->set_level(spdlog::level::err);
	} else if (strcmp(level, "CRITICAL") == 0) {		
		log->set_level(spdlog::level::critical);
	} else if (strcmp(level, "ALERT") == 0) {		
		log->set_level(spdlog::level::alert);
	} else if (strcmp(level, "EMERG") == 0) {		
		log->set_level(spdlog::level::emerg);
	} else if (strcmp(level, "OFF") == 0) {		
		log->set_level(spdlog::level::off);
	}
}

}