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

final class ObservableConcurrencyTests: XCTestCase {

    func test_singleFromAsync_emitsSuccess() async {
        let success = expectation(description: "Single emitted success")

        _ = Single<Int>.fromAsync {
            return 1
        }
        .subscribe(
            onSuccess: { value in
                XCTAssertEqual(value, 1)
                success.fulfill()
            },
            onFailure: { error in
                XCTFail("Unexpected error: \(error)")
            }
        )

        await fulfillment(of: [success], timeout: 1)
    }

    func test_singleFromAsync_emitsFailure() async {
        let failure = expectation(description: "Single emitted failure")

        _ = Single<Int>.fromAsync {
            throw ObservableConcurrencyTestError.error
        }
        .subscribe(
            onSuccess: { value in
                XCTFail("Unexpected value: \(value)")
            },
            onFailure: { error in
                guard case ObservableConcurrencyTestError.error = error else {
                    XCTFail("Unexpected error: \(error)")
                    return
                }
                failure.fulfill()
            }
        )

        await fulfillment(of: [failure], timeout: 1)
    }

    func test_singleFromAsync_cancelsTaskWhenDisposed() async {
        let taskStarted = expectation(description: "Task started")
        let taskCancelled = expectation(description: "Task cancelled")
        let disposable = Single<Int>.fromAsync {
            taskStarted.fulfill()
            while !Task.isCancelled {
                await Task.yield()
            }
            taskCancelled.fulfill()
            throw CancellationError()
        }
        .subscribe()

        await fulfillment(of: [taskStarted], timeout: 1)
        disposable.dispose()
        await fulfillment(of: [taskCancelled], timeout: 1)
    }

    func test_observableFromAsync_emitsValueAndCompletes() async {
        let completed = expectation(description: "Observable completed")
        var receivedValues: [Int] = []

        _ = Observable<Int>.fromAsync {
            return 1
        }
        .subscribe(
            onNext: { value in
                receivedValues.append(value)
            },
            onError: { error in
                XCTFail("Unexpected error: \(error)")
            },
            onCompleted: {
                completed.fulfill()
            }
        )

        await fulfillment(of: [completed], timeout: 1)
        XCTAssertEqual(receivedValues, [1])
    }

    func test_mapAsync_emitsValuesInOrder() async {
        let completed = expectation(description: "Observable completed")
        var receivedValues: [Int] = []

        _ = Observable.from([1, 2, 3])
            .mapAsync { value in
                value * 2
            }
            .subscribe(
                onNext: { value in
                    receivedValues.append(value)
                },
                onError: { error in
                    XCTFail("Unexpected error: \(error)")
                },
                onCompleted: {
                    completed.fulfill()
                }
            )

        await fulfillment(of: [completed], timeout: 1)
        XCTAssertEqual(receivedValues, [2, 4, 6])
    }

    func test_flatMapLatestAsync_cancelsInFlightTaskWhenNewElementArrives() async {
        let firstTaskStarted = expectation(description: "First task started")
        let firstTaskCancelled = expectation(description: "First task cancelled")
        let valueReceived = expectation(description: "Latest value received")
        let subject = PublishSubject<Int>()
        var receivedValues: [Int] = []

        let disposable = subject
            .flatMapLatestAsync { value in
                if value == 1 {
                    firstTaskStarted.fulfill()
                    while !Task.isCancelled {
                        await Task.yield()
                    }
                    firstTaskCancelled.fulfill()
                    throw CancellationError()
                }
                return value * 10
            }
            .subscribe(
                onNext: { value in
                    receivedValues.append(value)
                    valueReceived.fulfill()
                },
                onError: { error in
                    XCTFail("Unexpected error: \(error)")
                }
            )

        subject.onNext(1)
        await fulfillment(of: [firstTaskStarted], timeout: 1)
        subject.onNext(2)
        await fulfillment(of: [firstTaskCancelled, valueReceived], timeout: 1)

        XCTAssertEqual(receivedValues, [20])
        disposable.dispose()
    }
}

private enum ObservableConcurrencyTestError: Error {
    case error
}
