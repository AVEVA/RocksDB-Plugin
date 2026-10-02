#pragma once

#include "BlobRequestHelpers.hpp"

#include <libxml/parser.h>
#include <libxml/tree.h>
#include <libxml/xmlstring.h>

#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

// Thin, hardened wrappers over libxml2 used by the response parsers. Documents are parsed without network
// access, DTDs or entity substitution.
namespace AVEVA::AzureClient::Private
{
    using XmlNode = xmlNode;

    [[nodiscard]] std::string_view GetLocalName(std::string_view value) noexcept;
    [[nodiscard]] bool MatchesLocalName(std::string_view qualifiedName, std::string_view localName) noexcept;
    // libxml2 strings are UTF-8 bytes typed as unsigned char.
    [[nodiscard]] std::string_view XmlText(const xmlChar* value) noexcept;
    [[nodiscard]] std::string_view NodeName(const XmlNode& node) noexcept;

    struct XmlDocDeleter
    {
        void operator()(xmlDoc* document) const noexcept
        {
            xmlFreeDoc(document);
        }
    };

    class XmlDocument
    {
      public:
        explicit XmlDocument(std::unique_ptr<xmlDoc, XmlDocDeleter> document) noexcept : m_document(std::move(document))
        {
        }

        [[nodiscard]] const XmlNode* Root() const noexcept
        {
            return xmlDocGetRootElement(m_document.get());
        }

      private:
        std::unique_ptr<xmlDoc, XmlDocDeleter> m_document;
    };

    // Calls `visit(child)` for each element child of `node`.
    template <class TVisitor> void ForEachElement(const XmlNode& node, const TVisitor& visit)
    {
        for (const XmlNode* child = node.children; child != nullptr; child = child->next)
        {
            if (child->type == XML_ELEMENT_NODE)
            {
                visit(*child);
            }
        }
    }

    // Tags give the description and root-name their own types so neither is
    // adjacent-and-same-type with another std::string_view parameter.
    struct XmlDescriptionTag
    {
    };

    using XmlDescription = StringLabel<XmlDescriptionTag>;

    struct XmlRootNameTag
    {
    };

    using XmlRootName = StringLabel<XmlRootNameTag>;

    // Parses `xml` strictly; returns null for anything that is not a well-formed document without a DOCTYPE.
    [[nodiscard]] std::unique_ptr<XmlDocument> TryReadXml(std::string_view xml);
    // A successful response body must be a well-formed document whose root element has the local name
    // `rootName`; anything else is an invalid response (std::invalid_argument).
    [[nodiscard]] std::unique_ptr<XmlDocument> TryReadXmlOrThrow(std::string_view xml,
        XmlDescription description,
        XmlRootName rootName);

    [[nodiscard]] const XmlNode* FindChildByLocalName(const XmlNode& node, std::string_view localName) noexcept;
    // The concatenated text and CDATA children of `node` (entity and character references already decoded).
    [[nodiscard]] std::string GetNodeText(const XmlNode& node);
    [[nodiscard]] std::optional<std::string> GetAttribute(const XmlNode& node, std::string_view localName);
    [[nodiscard]] std::string GetChildTextOrEmpty(const XmlNode& node, std::string_view tag);
    [[nodiscard]] std::vector<const XmlNode*> GetChildrenByLocalName(const XmlNode& node, std::string_view localName);
} // namespace AVEVA::AzureClient::Private