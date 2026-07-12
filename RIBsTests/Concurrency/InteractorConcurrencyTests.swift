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

import XCTest
@testable import RIBs

final class InteractorConcurrencyTests: XCTestCase {

    func test_taskCancelOnDeactivate_cancelsTaskWhenInteractorDeactivates() async {
        let interactor = Interactor()
        interactor.activate()
        let taskCancelled = expectation(description: "Task cancelled")

        let task = Task {
            while !Task.isCancelled {
                await Task.yield()
            }
            taskCancelled.fulfill()
        }
        .cancelOnDeactivate(interactor: interactor)
        XCTAssertFalse(task.isCancelled)

        interactor.deactivate()
        await fulfillment(of: [taskCancelled], timeout: 1)

        XCTAssertTrue(task.isCancelled)
    }

    func test_taskCancelOnDeactivate_cancelsImmediatelyWhenInteractorIsInactive() async {
        let interactor = Interactor()
        let taskCancelled = expectation(description: "Task cancelled")

        let task = Task {
            while !Task.isCancelled {
                await Task.yield()
            }
            taskCancelled.fulfill()
        }
        .cancelOnDeactivate(interactor: interactor)

        await fulfillment(of: [taskCancelled], timeout: 1)
        XCTAssertTrue(task.isCancelled)
    }

    func test_throwingTaskCancelOnDeactivate_cancelsTaskWhenInteractorDeactivates() async {
        let interactor = Interactor()
        interactor.activate()

        let task = Task {
            try await Task.sleep(nanoseconds: 10_000_000_000)
        }
        .cancelOnDeactivate(interactor: interactor)
        XCTAssertFalse(task.isCancelled)

        interactor.deactivate()

        do {
            try await task.value
            XCTFail("Expected task to throw CancellationError")
        } catch is CancellationError {
            // Expected.
        } catch {
            XCTFail("Unexpected error: \(error)")
        }

        XCTAssertTrue(task.isCancelled)
    }
}
