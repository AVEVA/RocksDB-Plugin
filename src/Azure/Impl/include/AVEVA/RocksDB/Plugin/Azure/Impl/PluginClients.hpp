// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once
#include "AVEVA/RocksDB/Plugin/Azure/Impl/TokenCredentials.hpp"

#include <AVEVA/AzureClient/BlobClient.hpp>
#include <AVEVA/AzureClient/BlobContainerClient.hpp>
#include <AVEVA/AzureClient/BlobServiceClient.hpp>
#include <AVEVA/AzureClient/PageBlobClient.hpp>
#include <AVEVA/HttpClient/HttpClient.hpp>

namespace AVEVA::RocksDB::Plugin::Azure::Impl {
// The Azure clients the plugin uses over the HTTP transport `THttp`, authenticating with the plugin's credential.
template <AzureClient::HttpClientLike THttp>
using BlobClientOn = AzureClient::BasicBlobClient<THttp, BasicPluginCredential<THttp>>;
template <AzureClient::HttpClientLike THttp>
using PageBlobClientOn = AzureClient::BasicPageBlobClient<THttp, BasicPluginCredential<THttp>>;
template <AzureClient::HttpClientLike THttp>
using BlobContainerClientOn = AzureClient::BasicBlobContainerClient<THttp, BasicPluginCredential<THttp>>;
template <AzureClient::HttpClientLike THttp>
using BlobServiceClientOn = AzureClient::BasicBlobServiceClient<THttp, BasicPluginCredential<THttp>>;

// The same over the real HTTP transport.
using BlobClient = AzureClient::BasicBlobClient<::AVEVA::HttpClient, PluginCredential>;
using PageBlobClient = AzureClient::BasicPageBlobClient<::AVEVA::HttpClient, PluginCredential>;
using BlobContainerClient = AzureClient::BasicBlobContainerClient<::AVEVA::HttpClient, PluginCredential>;
using BlobServiceClient = AzureClient::BasicBlobServiceClient<::AVEVA::HttpClient, PluginCredential>;
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl

// Instantiated once, in PluginClients.cpp, so the plugin does not recompile the client implementations per file.
namespace AVEVA::AzureClient {
extern template class BasicBlobClient<::AVEVA::HttpClient, RocksDB::Plugin::Azure::Impl::PluginCredential>;
extern template class BasicPageBlobClient<::AVEVA::HttpClient, RocksDB::Plugin::Azure::Impl::PluginCredential>;
extern template class BasicBlobContainerClient<::AVEVA::HttpClient, RocksDB::Plugin::Azure::Impl::PluginCredential>;
extern template class BasicBlobServiceClient<::AVEVA::HttpClient, RocksDB::Plugin::Azure::Impl::PluginCredential>;
} // namespace AVEVA::AzureClient
