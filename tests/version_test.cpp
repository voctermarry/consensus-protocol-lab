#include <cstdlib>
#include <iostream>
#include <string_view>

#include "consensus_lab/version.hpp"

namespace {

int failures = 0;

void expect(bool ok, std::string_view what) {
  if (!ok) {
    ++failures;
    std::cerr << "断言失败: " << what << '\n';
  }
}

}  // namespace

int main() {
  expect(!consensus_lab::version().empty(), "version() 非空");
  expect(consensus_lab::version() == consensus_lab::kVersion, "version() 与 kVersion 一致");
  if (failures != 0) {
    std::cerr << failures << " 项断言失败\n";
    return 1;
  }
  std::cout << "全部断言通过\n";
  return 0;
}
