#pragma once

#include <string>

namespace AVEVA
{
    class HttpHeader
    {
      public:
        HttpHeader();
        HttpHeader(std::string name, std::string value);

        const std::string& GetName() const noexcept;
        const std::string& GetValue() const noexcept;
        void SetName(std::string name);
        void SetValue(std::string value);

      private:
        std::string m_name;
        std::string m_value;
    };
} // namespace AVEVA
