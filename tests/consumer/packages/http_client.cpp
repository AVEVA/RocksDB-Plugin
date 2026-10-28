#include <AVEVA/HttpClient/HttpMethod.hpp>

int main(int argc, char**) {
    if (argc > 100) {
        return static_cast<int>(AVEVA::ToString(AVEVA::HttpMethod::Get).size());
    }
    return 0;
}
