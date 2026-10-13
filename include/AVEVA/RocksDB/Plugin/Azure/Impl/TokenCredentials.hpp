// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once
#include "AVEVA/RocksDB/Plugin/Azure/Impl/ClientRuntime.hpp"

#include <AVEVA/AzureClient/BlobStorageErrorCode.hpp>
#include <AVEVA/AzureClient/ChainedTokenCredential.hpp>
#include <AVEVA/AzureClient/CredentialVariant.hpp>
#include <AVEVA/AzureClient/Credentials.hpp>
#include <AVEVA/AzureClient/TokenCredential.hpp>
#include <AVEVA/HttpClient/HttpClient.hpp>
#include <AVEVA/HttpClient/HttpHeader.hpp>
#include <AVEVA/HttpClient/HttpMethod.hpp>
#include <AVEVA/HttpClient/HttpRequest.hpp>
#include <AVEVA/HttpClient/HttpRequestOptions.hpp>
#include <AVEVA/HttpClient/HttpResponse.hpp>

#include <boost/json/parse.hpp>
#include <boost/json/value.hpp>
#include <boost/log/trivial.hpp>

#include <algorithm>
#include <concepts>
#include <cstddef>
#include <functional>
#include <memory>
#include <string>
#include <system_error>
#include <typeinfo>
#include <utility>
#include <variant>
#include <vector>
namespace AVEVA::RocksDB::Plugin::Azure::Impl {
using AzureClient::AzurePipelinesCredentialOptions;
using AzureClient::BasicAzurePipelinesCredential;
using AzureClient::ChainedTokenCredential;
using AzureClient::CredentialVariant;

/// <summary>The credentials a single token source can be, over the HTTP transport `THttp`.</summary>
template <AzureClient::HttpClientLike THttp>
using BasicLeafCredential =
    CredentialVariant<AzureClient::BasicClientSecretCredential<THttp>,
                      AzureClient::BasicWorkloadIdentityCredential<THttp>,
                      AzureClient::BasicManagedIdentityCredential<THttp>, BasicAzurePipelinesCredential<THttp>>;

/// <summary>A leaf credential, or a chain of them.</summary>
template <AzureClient::HttpClientLike THttp>
using BasicSourceCredential =
    CredentialVariant<AzureClient::BasicClientSecretCredential<THttp>,
                      AzureClient::BasicWorkloadIdentityCredential<THttp>,
                      AzureClient::BasicManagedIdentityCredential<THttp>, BasicAzurePipelinesCredential<THttp>,
                      ChainedTokenCredential<BasicLeafCredential<THttp>>>;

/// <summary>
/// The one credential type every plugin client over `THttp` is instantiated with: any source, cached.
/// </summary>
template <AzureClient::HttpClientLike THttp>
using BasicPluginCredential = AzureClient::CachingTokenCredential<BasicSourceCredential<THttp>>;

using LeafCredential = BasicLeafCredential<::AVEVA::HttpClient>;
using SourceCredential = BasicSourceCredential<::AVEVA::HttpClient>;
using PluginCredential = BasicPluginCredential<::AVEVA::HttpClient>;
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl
