#include "BlobXmlParser.hpp"

#include <limits>
#include <mutex>
#include <new>
#include <stdexcept>

namespace AVEVA::AzureClient::Private
{
    namespace
    {
        struct XmlParserCtxtDeleter
        {
            void operator()(xmlParserCtxt* context) const noexcept
            {
                xmlFreeParserCtxt(context);
            }
        };

        // No network access and no diagnostics on stderr. Deliberately absent: XML_PARSE_NOENT and XML_PARSE_DTDLOAD
        // (entity substitution / external DTD loading) and XML_PARSE_HUGE, which would lift libxml2's default
        // nesting-depth and text-size limits that protect against stack exhaustion and memory blow-up.
        constexpr int XmlParseOptions = XML_PARSE_NONET | XML_PARSE_NOERROR | XML_PARSE_NOWARNING | XML_PARSE_COMPACT;

        void InitializeXmlParser()
        {
            static std::once_flag once;
            std::call_once(once,
                []
            {
                xmlInitParser();
            });
        }
    } // namespace

    [[nodiscard]] std::string_view GetLocalName(std::string_view value) noexcept
    {
        const std::size_t separator = value.rfind(':');
        return separator == std::string_view::npos ? value : value.substr(separator + 1U);
    }

    [[nodiscard]] bool MatchesLocalName(std::string_view qualifiedName, std::string_view localName) noexcept
    {
        return GetLocalName(qualifiedName) == localName;
    }

    [[nodiscard]] std::string_view XmlText(const xmlChar* value) noexcept
    {
        return value == nullptr ? std::string_view{} : std::string_view{reinterpret_cast<const char*>(value)};
    }

    [[nodiscard]] std::string_view NodeName(const XmlNode& node) noexcept
    {
        return XmlText(node.name);
    }

    // Parses `xml` strictly (well-formedness errors, including mismatched end tags and undeclared entities, reject
    // the document). Returns null for anything that is not a well-formed document without a DOCTYPE: service
    // responses never carry one, so refusing it removes DTD processing (entity declarations, external subsets)
    // entirely.
    [[nodiscard]] std::unique_ptr<XmlDocument> TryReadXml(std::string_view xml)
    {
        if (xml.size() > static_cast<std::size_t>(std::numeric_limits<int>::max()))
        {
            return nullptr;
        }

        InitializeXmlParser();
        const std::unique_ptr<xmlParserCtxt, XmlParserCtxtDeleter> context{xmlNewParserCtxt()};
        if (!context)
        {
            throw std::bad_alloc();
        }

        std::unique_ptr<xmlDoc, XmlDocDeleter> document{xmlCtxtReadMemory(context.get(),
            xml.data(),
            static_cast<int>(xml.size()),
            nullptr,
            nullptr,
            XmlParseOptions)};
        if (!document || xmlGetIntSubset(document.get()) != nullptr)
        {
            return nullptr;
        }
        return std::make_unique<XmlDocument>(std::move(document));
    }

    // A successful response body must be a well-formed document whose root element has the local name
    // `rootName`; anything else (including an empty or truncated body) is an invalid response.
    [[nodiscard]] std::unique_ptr<XmlDocument> TryReadXmlOrThrow(std::string_view xml,
        XmlDescription description,
        XmlRootName rootName)
    {
        auto document = TryReadXml(xml);
        if (!document)
        {
            throw std::invalid_argument(std::string(description.Value) + " must be well-formed XML.");
        }
        const XmlNode* root = document->Root();
        if (root == nullptr || !MatchesLocalName(NodeName(*root), rootName.Value))
        {
            throw std::invalid_argument(
                std::string(description.Value) + " must have a <" + std::string(rootName.Value) + "> root element.");
        }

        return document;
    }

    [[nodiscard]] const XmlNode* FindChildByLocalName(const XmlNode& node, std::string_view localName) noexcept
    {
        for (const XmlNode* child = node.children; child != nullptr; child = child->next)
        {
            if (child->type == XML_ELEMENT_NODE && MatchesLocalName(NodeName(*child), localName))
            {
                return child;
            }
        }
        return nullptr;
    }

    // The concatenated text and CDATA children of `node` (entity and character references already decoded).
    [[nodiscard]] std::string GetNodeText(const XmlNode& node)
    {
        std::string text;
        for (const XmlNode* child = node.children; child != nullptr; child = child->next)
        {
            if (child->type == XML_TEXT_NODE || child->type == XML_CDATA_SECTION_NODE)
            {
                text.append(XmlText(child->content));
            }
        }
        return text;
    }

    [[nodiscard]] std::optional<std::string> GetAttribute(const XmlNode& node, std::string_view localName)
    {
        for (const xmlAttr* attribute = node.properties; attribute != nullptr; attribute = attribute->next)
        {
            if (MatchesLocalName(XmlText(attribute->name), localName))
            {
                std::string value;
                for (const XmlNode* child = attribute->children; child != nullptr; child = child->next)
                {
                    if (child->type == XML_TEXT_NODE)
                    {
                        value.append(XmlText(child->content));
                    }
                }
                return value;
            }
        }
        return std::nullopt;
    }

    [[nodiscard]] std::string GetChildTextOrEmpty(const XmlNode& node, std::string_view tag)
    {
        const XmlNode* child = FindChildByLocalName(node, tag);
        return child != nullptr ? GetNodeText(*child) : std::string{};
    }

    [[nodiscard]] std::vector<const XmlNode*> GetChildrenByLocalName(const XmlNode& node, std::string_view localName)
    {
        std::vector<const XmlNode*> children;
        ForEachElement(node,
            [&](const XmlNode& child)
        {
            if (MatchesLocalName(NodeName(child), localName))
            {
                children.push_back(&child);
            }
        });
        return children;
    }
} // namespace AVEVA::AzureClient::Private
