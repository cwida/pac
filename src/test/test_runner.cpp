//
// Created by ila on 12/24/25.
//

#include <iostream>
#include "include/test_runner.hpp"

int main() {
    std::cerr << "Starting unified test runner...\n";
    int code = 0;

    std::cerr << "Running compiler function tests...\n";
    code = RunCompilerFunctionTests();
    if (code != 0) {
        std::cerr << "RunCompilerFunctionTests failed with code " << code << "\n";
        return code;
    }

    std::cerr << "Running privacy columns tests...\n";
    code = RunPrivacyColumnsTests();
    if (code != 0) {
        std::cerr << "RunPrivacyColumnsTests failed with code " << code << "\n";
        return code;
    }

    std::cerr << "All tests passed\n";
    return 0;
}
