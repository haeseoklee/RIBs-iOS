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

public extension PrimitiveSequenceType where Trait == SingleTrait {

    /// Create a cold single that runs the given async work on subscription.
    static func fromAsync(_ work: @escaping () async throws -> Element) -> Single<Element> {
        return .create { observer in
            let task = Task {
                do {
                    observer(.success(try await work()))
                } catch {
                    observer(.failure(error))
                }
            }

            return Disposables.create {
                task.cancel()
            }
        }
    }
}

public extension ObservableType {

    /// Create a cold observable that runs the given async work on subscription.
    static func fromAsync(_ work: @escaping () async throws -> Element) -> Observable<Element> {
        return Single<Element>.fromAsync(work).asObservable()
    }

    /// Transform elements by running async work sequentially while preserving order.
    func mapAsync<Result>(_ transform: @escaping (Element) async throws -> Result) -> Observable<Result> {
        return concatMap { element in
            Single<Result>.fromAsync {
                try await transform(element)
            }
            .asObservable()
        }
    }

    /// Transform elements by running async work and cancelling in-flight work when a new element arrives.
    func flatMapLatestAsync<Result>(_ transform: @escaping (Element) async throws -> Result) -> Observable<Result> {
        return flatMapLatest { element in
            Single<Result>.fromAsync {
                try await transform(element)
            }
            .asObservable()
        }
    }
}
#endif
