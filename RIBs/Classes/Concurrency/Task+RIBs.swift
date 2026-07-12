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

public extension Task {

    /// Cancel this task when the given interactor deactivates.
    @discardableResult
    func cancelOnDeactivate(interactor: Interactor) -> Task<Success, Failure> {
        Disposables.create {
            self.cancel()
        }
        .disposeOnDeactivate(interactor: interactor)
        return self
    }

    /// Cancel this task when the given worker stops.
    @discardableResult
    func cancelOnStop(_ worker: Worker) -> Task<Success, Failure> {
        Disposables.create {
            self.cancel()
        }
        .disposeOnStop(worker)
        return self
    }

    /// Cancel this task when the given workflow is disposed.
    @discardableResult
    func cancel<ActionableItemType>(with workflow: Workflow<ActionableItemType>) -> Task<Success, Failure> {
        Disposables.create {
            self.cancel()
        }
        .disposeWith(workflow: workflow)
        return self
    }
}
#endif
