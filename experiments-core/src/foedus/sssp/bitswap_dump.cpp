#include <stdint.h>

#include <iostream>

uint8_t slow_bitswap(uint8_t b) {
  uint8_t ret = 0;
  for (uint8_t pos = 0; pos < 8U; ++pos) {
    if (b & (1U << pos)) {
      ret |= (1U << (7U - pos));
    }
  }
  return ret;
}

int main(int /*argc*/, char **/*argv*/) {
  for (uint32_t i = 0; i < 256U; ++i) {
    std::cout << " ";
    std::cout << static_cast<int>(slow_bitswap(i));
    std::cout << ",";
    if (i % 16 == 15) {
      std::cout << std::endl << " ";
    }
  }
}
