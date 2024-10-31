

#pragma once

#include <jwt-cpp/jwt.h>

#include <cstdint>
#include <string>
#include <unordered_map>

namespace util {

std::string GenerateToken(
    const std::string& secret,
    const std::unordered_map<std::string, std::string>& claims);

bool VerifyToken(const std::string& secret, const std::string& token);

std::string GetClaim(const std::string& key);

}  // namespace util