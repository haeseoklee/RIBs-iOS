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

import RxSwift
import XCTest
@testable import RIBs

final class WorkflowConcurrencyTests: XCTestCase {

    func test_workflowTaskCancelWithWorkflow_cancelsWhenWorkflowDisposableIsDisposed() async {
        let workflow = Workflow<()>()
        let taskCancelled = expectation(description: "Task cancelled")
        let task = Task {
            while !Task.isCancelled {
                await Task.yield()
            }
            taskCancelled.fulfill()
        }
        .cancel(with: workflow)
        let disposable = workflow
            .onStep { _ in
                Observable.just(((), ()))
            }
            .commit()
            .subscribe(())

        XCTAssertFalse(task.isCancelled)
        disposable.dispose()
        await fulfillment(of: [taskCancelled], timeout: 1)

        XCTAssertTrue(task.isCancelled)
    }

    func test_throwingTaskCancelWithWorkflow_cancelsWhenWorkflowDisposableIsDisposed() async {
        let workflow = Workflow<()>()
        let task = Task {
            try await Task.sleep(nanoseconds: 10_000_000_000)
        }
        .cancel(with: workflow)
        let disposable = workflow
            .onStep { _ in
                Observable.just(((), ()))
            }
            .commit()
            .subscribe(())

        XCTAssertFalse(task.isCancelled)
        disposable.dispose()

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

    func test_workflowOnAsyncStep_emitsAsyncResultAndInvokesWorkflowDidComplete() async {
        let workflow = WorkflowConcurrencyTestWorkflow<String>()
        let stepRan = expectation(description: "Root async step ran")
        workflow.onComplete = {
            stepRan.fulfill()
        }

        _ = workflow
            .onAsyncStep { actionableItem in
                return (actionableItem.count, actionableItem)
            }
            .onStep { length, value in
                XCTAssertEqual(length, 4)
                XCTAssertEqual(value, "test")
                return Observable.just(((), ()))
            }
            .commit()
            .subscribe("test")

        await fulfillment(of: [stepRan], timeout: 1)
        XCTAssertEqual(workflow.completeCallCount, 1)
        XCTAssertEqual(workflow.errorCallCount, 0)
    }

    func test_workflowOnAsyncStep_invokesWorkflowDidReceiveError() async {
        let workflow = WorkflowConcurrencyTestWorkflow<Int>()
        let receivedError = expectation(description: "Workflow received error")
        workflow.onError = {
            receivedError.fulfill()
        }

        _ = workflow
            .onAsyncStep { _ -> (Int, Int) in
                throw WorkflowConcurrencyTestError.error
            }
            .commit()
            .subscribe(1)

        await fulfillment(of: [receivedError], timeout: 1)
        XCTAssertEqual(workflow.completeCallCount, 0)
        XCTAssertEqual(workflow.errorCallCount, 1)
    }

    func test_workflowOnAsyncStep_cancelsTaskWhenWorkflowDisposableIsDisposed() async {
        let workflow = Workflow<()>()
        let taskStarted = expectation(description: "Root async step task started")
        let taskCancelled = expectation(description: "Root async step task cancelled")
        let disposable = workflow
            .onAsyncStep { _ -> ((), ()) in
                taskStarted.fulfill()
                while !Task.isCancelled {
                    await Task.yield()
                }
                taskCancelled.fulfill()
                return ((), ())
            }
            .commit()
            .subscribe(())

        await fulfillment(of: [taskStarted], timeout: 1)
        disposable.dispose()
        await fulfillment(of: [taskCancelled], timeout: 1)
    }

    func test_onAsyncStep_emitsAsyncResult() async {
        let workflow = Workflow<Int>()
        let stepRan = expectation(description: "Async step ran")

        _ = workflow
            .onStep { actionableItem in
                Observable.just((actionableItem, actionableItem))
            }
            .onAsyncStep { actionableItem, value in
                return (actionableItem + 1, value + 2)
            }
            .onStep { actionableItem, value in
                XCTAssertEqual(actionableItem, 2)
                XCTAssertEqual(value, 3)
                stepRan.fulfill()
                return Observable.just(((), ()))
            }
            .commit()
            .subscribe(1)

        await fulfillment(of: [stepRan], timeout: 1)
    }

    func test_onAsyncStep_invokesWorkflowDidReceiveError() async {
        let workflow = WorkflowConcurrencyTestWorkflow<Int>()
        let receivedError = expectation(description: "Workflow received error")
        workflow.onError = {
            receivedError.fulfill()
        }

        _ = workflow
            .onStep { actionableItem in
                Observable.just((actionableItem, actionableItem))
            }
            .onAsyncStep { _, _ -> (Int, Int) in
                throw WorkflowConcurrencyTestError.error
            }
            .commit()
            .subscribe(1)

        await fulfillment(of: [receivedError], timeout: 1)
        XCTAssertEqual(workflow.completeCallCount, 0)
        XCTAssertEqual(workflow.errorCallCount, 1)
    }

    func test_onAsyncStep_cancelsTaskWhenWorkflowDisposableIsDisposed() async {
        let workflow = Workflow<()>()
        let taskStarted = expectation(description: "Async step task started")
        let taskCancelled = expectation(description: "Async step task cancelled")
        let disposable = workflow
            .onStep { _ in
                Observable.just(((), ()))
            }
            .onAsyncStep { _, _ -> ((), ()) in
                taskStarted.fulfill()
                while !Task.isCancelled {
                    await Task.yield()
                }
                taskCancelled.fulfill()
                return ((), ())
            }
            .commit()
            .subscribe(())

        await fulfillment(of: [taskStarted], timeout: 1)
        disposable.dispose()
        await fulfillment(of: [taskCancelled], timeout: 1)
    }
}

private enum WorkflowConcurrencyTestError: Error {
    case error
}

private final class WorkflowConcurrencyTestWorkflow<ActionableItemType>: Workflow<ActionableItemType> {
    var completeCallCount = 0
    var errorCallCount = 0
    var forkCallCount = 0
    var receivedError: Error?
    var onComplete: (() -> Void)?
    var onError: (() -> Void)?

    override func didComplete() {
        completeCallCount += 1
        onComplete?()
    }

    override func didFork() {
        forkCallCount += 1
    }

    override func didReceiveError(_ error: Error) {
        errorCallCount += 1
        receivedError = error
        onError?()
    }
}
