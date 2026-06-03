//
//  Copyright (c) 2017. Uber Technologies
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//

#if swift(>=5.6) && canImport(_Concurrency)

import RxSwift

public extension AsyncSequence {

    /// Confines the async sequence's elements to the given interactor scope.
    ///
    /// Elements are only yielded while the interactor scope is active. While the sequence is being iterated,
    /// values emitted while inactive are ignored except for the latest value, which can be emitted when the scope
    /// becomes active again.
    ///
    /// - parameter interactorScope: The interactor scope whose activeness this async sequence is confined to.
    /// - returns: The async throwing stream confined to this interactor's activeness lifecycle.
    func confineTo(_ interactorScope: InteractorScope) -> AsyncThrowingStream<Element, Error> {
        return asObservable()
            .confineTo(interactorScope)
            .values
    }
}

#endif
