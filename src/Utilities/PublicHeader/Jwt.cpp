#include "crane/Jwt.h"

namespace util {
std::string GenerateToken(
    const std::string& secret,
    const std::unordered_map<std::string, std::string>& claims) {
  auto creater = jwt::create().set_issuer("crane").set_type("JWS");
  for (const auto& [k, v] : claims) {
    creater.set_payload_claim(k, jwt::claim(v));
  }
  return creater.sign(jwt::algorithm::hs256{secret});
}

bool VerifyToken(const std::string& secret, const std::string& token) {
  try {
    auto decoded = jwt::decode(token);
    jwt::verify()
        .allow_algorithm(jwt::algorithm::hs256{secret})
        .with_issuer("crane")
        .verify(decoded);
  } catch (std::invalid_argument& a) {
    return false;
  } catch (jwt::error::token_verification_exception& e) {
    return false;
  }

  return true;
}

std::string GetClaim(const std::string& key, const std::string& token) {
  auto decoded = jwt::decode(token);

  return decoded.get_payload_claim(key).as_string();
}

}  // namespace util
