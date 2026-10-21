// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "IdleSweeper.hpp"

namespace AVEVA::Private {
void SweeperNotifier::operator()() const noexcept {
    if (auto sweeper = m_sweeper.lock()) {
        sweeper->Notify();
    }
}
} // namespace AVEVA::Private
