// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include <AVEVA/AzureClient/Detail/BlobRequestHelpers.hpp>

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
namespace AVEVA::AzureClient::Private {
/// <summary>
/// A libxml2 tree node.
/// </summary>
using XmlNode = xmlNode;

/// <summary>
/// Strips a namespace prefix, so documents parse the same whichever prefix the service used.
/// </summary>
/// <param name="value">A possibly prefixed name such as <c>ns:Name</c>.</param>
/// <returns>The part after the last ':'; a view into <paramref name="value"/>.</returns>
[[nodiscard]] std::string_view GetLocalName(std::string_view value) noexcept;

/// <summary>
/// Compares a possibly prefixed name with a local name, ignoring the prefix.
/// </summary>
/// <param name="qualifiedName">The name from the document.</param>
/// <param name="localName">The local name to compare with.</param>
/// <returns><c>StringMatch::Match</c> when the local part of <paramref name="qualifiedName"/> equals
/// <paramref name="localName"/>.</returns>
[[nodiscard]] StringMatch MatchesLocalName(std::string_view qualifiedName, std::string_view localName) noexcept;

/// <summary>
/// libxml2 strings are UTF-8 bytes typed as unsigned char.
/// </summary>
/// <param name="value">The libxml2 string; may be null.</param>
/// <returns>A view of the bytes, or an empty view when <paramref name="value"/> is null.</returns>
[[nodiscard]] std::string_view XmlText(const xmlChar* value) noexcept;

/// <summary>
/// The name of a node, prefix included.
/// </summary>
/// <param name="node">The node.</param>
/// <returns>A view into the document; empty when the node has no name.</returns>
[[nodiscard]] std::string_view NodeName(const XmlNode& node) noexcept;

/// <summary>
/// Frees a libxml2 document, so it can be owned by a <c>std::unique_ptr</c>.
/// </summary>
struct XmlDocDeleter {
    /// <summary>
    /// Frees the document.
    /// </summary>
    /// <param name="document">The document to free.</param>
    void operator()(xmlDoc* document) const noexcept { xmlFreeDoc(document); }
};

/// <summary>
/// Owns a parsed libxml2 document, and with it every node and string view handed out from it.
/// </summary>
class XmlDocument {
  public:
    /// <summary>
    /// Takes ownership of a parsed document.
    /// </summary>
    /// <param name="document">The parsed document.</param>
    explicit XmlDocument(std::unique_ptr<xmlDoc, XmlDocDeleter> document) noexcept : m_document(std::move(document)) {}

    /// <summary>
    /// The document's root element.
    /// </summary>
    /// <returns>The root, valid for the document's lifetime; null when the document has none.</returns>
    [[nodiscard]] const XmlNode* Root() const noexcept { return xmlDocGetRootElement(m_document.get()); }

  private:
    std::unique_ptr<xmlDoc, XmlDocDeleter> m_document;
};

/// <summary>
/// Calls <c>visit(child)</c> for each element child of <c>node</c>.
/// </summary>
/// <typeparam name="TVisitor">A callable taking <c>const XmlNode&amp;</c>.</typeparam>
/// <param name="node">The parent node.</param>
/// <param name="visit">The callable invoked with each element child, in document order.</param>
template <class TVisitor> void ForEachElement(const XmlNode& node, const TVisitor& visit) {
    for (const XmlNode* child = node.children; child != nullptr; child = child->next) {
        if (child->type == XML_ELEMENT_NODE) {
            visit(*child);
        }
    }
}

/// <summary>
/// Tags give the description and root-name their own types so neither is
/// adjacent-and-same-type with another std::string_view parameter.
/// </summary>
struct XmlDescriptionTag {};

/// <summary>
/// Names what a document is, for the message of the error raised when it is invalid.
/// </summary>
using XmlDescription = StringLabel<XmlDescriptionTag>;

/// <summary>
/// Tag that gives <see cref="XmlRootName"/> its own type; see <see cref="XmlDescriptionTag"/>.
/// </summary>
struct XmlRootNameTag {};

/// <summary>
/// The local name the root element of a document is required to have.
/// </summary>
using XmlRootName = StringLabel<XmlRootNameTag>;

/// <summary>
/// Parses <c>xml</c> strictly; returns null for anything that is not a well-formed document without a DOCTYPE.
/// </summary>
/// <param name="xml">The document text.</param>
/// <returns>The parsed document, or null when it is empty, malformed or has a DOCTYPE.</returns>
/// <exception cref="std::bad_alloc">Thrown when a parser context cannot be allocated.</exception>
[[nodiscard]] std::unique_ptr<XmlDocument> TryReadXml(std::string_view xml);

/// <summary>
/// A successful response body must be a well-formed document whose root element has the local name
/// <c>rootName</c>; anything else is an invalid response (std::invalid_argument).
/// </summary>
/// <param name="xml">The response body.</param>
/// <param name="description">Names the document in the error message.</param>
/// <param name="rootName">The required local name of the root element.</param>
/// <returns>The parsed document.</returns>
/// <exception cref="std::invalid_argument">Thrown when the body is not well-formed XML or its root element is not
/// <paramref name="rootName"/>.</exception>
[[nodiscard]] std::unique_ptr<XmlDocument> TryReadXmlOrThrow(std::string_view xml, XmlDescription description,
                                                             XmlRootName rootName);

/// <summary>
/// Finds the first element child with a given local name.
/// </summary>
/// <param name="node">The parent node.</param>
/// <param name="localName">The local name to look for, ignoring any prefix.</param>
/// <returns>The child, or null when there is none.</returns>
[[nodiscard]] const XmlNode* FindChildByLocalName(const XmlNode& node, std::string_view localName) noexcept;

/// <summary>
/// The concatenated text and CDATA children of <c>node</c> (entity and character references already decoded).
/// </summary>
/// <param name="node">The node to read.</param>
/// <returns>The text; empty when the node has none.</returns>
[[nodiscard]] std::string GetNodeText(const XmlNode& node);

/// <summary>
/// Zero-copy variant of GetNodeText; nullopt when the text spans several children.
/// </summary>
/// <param name="node">The node to read.</param>
/// <returns>A view into the document, which must outlive it; nullopt when the text must be concatenated.</returns>
[[nodiscard]] std::optional<std::string_view> GetNodeTextView(const XmlNode& node) noexcept;

/// <summary>
/// Reads an attribute by local name.
/// </summary>
/// <param name="node">The element carrying the attribute.</param>
/// <param name="localName">The local name of the attribute, ignoring any prefix.</param>
/// <returns>The attribute's text; nullopt when the element has no such attribute.</returns>
[[nodiscard]] std::optional<std::string> GetAttribute(const XmlNode& node, std::string_view localName);

/// <summary>
/// Reads the text of a child element, tolerating its absence.
/// </summary>
/// <param name="node">The parent node.</param>
/// <param name="tag">The local name of the child element.</param>
/// <returns>The child's text; empty when there is no such child.</returns>
[[nodiscard]] std::string GetChildTextOrEmpty(const XmlNode& node, std::string_view tag);
} // namespace AVEVA::AzureClient::Private