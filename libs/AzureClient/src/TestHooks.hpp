#pragma once

#include <cstddef>

namespace AVEVA::AzureClient::TestHooks
{
    void ResetUploadBufferAllocationCounter();
    std::size_t GetTotalUploadBufferAllocations();

    void ResetPeakUploadBufferBytes();
    std::size_t GetPeakUploadBufferBytes();
} // namespace AVEVA::AzureClient::TestHooks
