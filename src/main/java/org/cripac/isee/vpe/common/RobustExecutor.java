/*
 * This file is part of las-vpe-platform.
 *
 * las-vpe-platform is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * las-vpe-platform is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with las-vpe-platform. If not, see <http://www.gnu.org/licenses/>.
 *
 * Created by ken.yu on 17-2-22.
 */

package org.cripac.isee.vpe.common;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function0;
import org.apache.spark.api.java.function.VoidFunction;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * The class RobustExecutor execute functions and automatically retry on failure.
 * It is recommended to only wrap codes that can easily fail,
 * and retrying might possibly fix the problem.
 */
public class RobustExecutor<T, R> {

    private final int maxRetries;
    private final int retryInterval;
    private Function<T, R> onceFunction;
    private List<Class<?>> noRetryThrowables;

    public interface VoidFunction0 {
        void call() throws Exception;
    }

    /**
     * Create a RobustExecutor specifying the retrying behaviour.
     * The executor retries immediately after failure,
     * and may retry up to 2 times (totally executing 3 times).
     *
     * @param onceFunction function to be executed each time.
     */
    public RobustExecutor(VoidFunction0 onceFunction) {
        this((VoidFunction<T>) t -> onceFunction.call());
    }

    /**
     * Create a RobustExecutor specifying the retrying behaviour.
     * The executor retries immediately after failure,
     * and may retry up to 2 times (totally executing 3 times).
     *
     * @param onceFunction function to be executed each time.
     */
    public RobustExecutor(VoidFunction<T> onceFunction) {
        this((Function<T, R>) param -> {
            onceFunction.call(param);
            return null;
        }, 2);
    }

    /**
     * Create a RobustExecutor specifying the retrying behaviour.
     * The executor retries immediately after failure,
     * and may retry up to 2 times (totally executing 3 times).
     * Note that be careful when simplifying the lambda expression for the function here,
     * you may not get the correct function type as expected, resulting in null return value.
     *
     * @param onceFunction function to be executed each time.
     */
    public RobustExecutor(Function0<R> onceFunction) {
        this((Function<T, R>) ignored -> onceFunction.call());
    }

    /**
     * Create a RobustExecutor specifying the retrying behaviour.
     * The executor retries immediately after failure,
     * and may retry up to 2 times (totally executing 3 times).
     * Note that be careful when simplifying the lambda expression for the function here,
     * you may not get the correct function type as expected, resulting in null return value.
     *
     * @param onceFunction function to be executed each time.
     */
    public RobustExecutor(Function<T, R> onceFunction) {
        this(onceFunction, 2);
    }

    /**
     * Create a RobustExecutor specifying the retrying behaviour.
     * The executor retries immediately after failure.
     * Note that be careful when simplifying the lambda expression for the function here,
     * you may not get the correct function type as expected, resulting in null return value.
     *
     * @param onceFunction function to be executed each time.
     * @param maxRetries   max times of retrying.
     */
    public RobustExecutor(Function<T, R> onceFunction, int maxRetries) {
        this(onceFunction, maxRetries, 0);
    }

    /**
     * Create a RobustExecutor specifying the retrying behaviour.
     * Note that be careful when simplifying the lambda expression for the function here,
     * you may not get the correct function type as expected, resulting in null return value.
     *
     * @param onceFunction  function to be executed each time.
     * @param maxRetries    max times of retrying.
     * @param retryInterval interval (ms) between retries.
     */
    public RobustExecutor(Function<T, R> onceFunction, int maxRetries, int retryInterval) {
        this(onceFunction, maxRetries, retryInterval, Collections.emptyList());
    }

    /**
     * Create a RobustExecutor specifying the retrying behaviour.
     * The executor retries immediately after failure,
     * and may retry up to 2 times (totally executing 3 times).
     *
     * @param onceFunction      function to be executed each time.
     * @param noRetryThrowables exceptions that are not able to be solved by retrying.
     */
    public RobustExecutor(VoidFunction0 onceFunction, List<Class<?>> noRetryThrowables) {
        this((VoidFunction<T>) param -> onceFunction.call(), noRetryThrowables);
    }

    /**
     * Create a RobustExecutor specifying the retrying behaviour.
     * The executor retries immediately after failure,
     * and may retry up to 2 times (totally executing 3 times).
     *
     * @param onceFunction      function to be executed each time.
     * @param noRetryThrowables exceptions that are not able to be solved by retrying.
     */
    public RobustExecutor(VoidFunction<T> onceFunction, List<Class<?>> noRetryThrowables) {
        this((Function<T, R>) param -> {
            onceFunction.call(param);
            return null;
        }, noRetryThrowables);
    }

    /**
     * Create a RobustExecutor specifying the retrying behaviour.
     * The executor retries immediately after failure,
     * and may retry up to 2 times (totally executing 3 times).
     *
     * @param onceFunction      function to be executed each time.
     * @param noRetryThrowables exceptions that are not able to be solved by retrying.
     */
    public RobustExecutor(Function<T, R> onceFunction, List<Class<?>> noRetryThrowables) {
        this(onceFunction, 2, 0, noRetryThrowables);
    }

    /**
     * Create a RobustExecutor specifying the retrying behaviour.
     * Note that be careful when simplifying the lambda expression for the function here,
     * you may not get the correct function type as expected, resulting in null return value.
     *
     * @param maxRetries        max times of retrying.
     * @param retryInterval     interval (ms) between retries.
     * @param noRetryThrowables exceptions that are not able to be solved by retrying.
     */
    public RobustExecutor(Function<T, R> onceFunction,
                          int maxRetries,
                          int retryInterval,
                          List<Class<?>> noRetryThrowables) {
        assert maxRetries >= 0;
        assert retryInterval >= 0;
        this.onceFunction = onceFunction;
        this.maxRetries = maxRetries;
        this.retryInterval = retryInterval;
        this.noRetryThrowables = noRetryThrowables;
    }

    /**
     * Execute the specified function with no parameter robustly.
     * When catching exceptions from the function, retry the execution.
     * When reaching the max retrying times, throw the exception to handle outside.
     * Note that be careful when simplifying the lambda expression for the function here,
     * you may not get the correct function type as expected, resulting in null return value.
     *
     * @throws Exception On failure that cannot be handled by retrying.
     */
    @Nonnull
    public R execute() throws Exception {
        return execute(null);
    }

    /**
     * Execute the specified function robustly.
     * When catching exceptions from the function, retry the execution.
     * When reaching the max retrying times, throw the exception to handle outside.
     * Note that be careful when simplifying the lambda expression for the function here,
     * you may not get the correct function type as expected, resulting in null return value.
     *
     * @throws Exception On failure that cannot be handled by retrying.
     */
    @Nonnull
    public R execute(@Nullable T param) throws Exception {
        int retryCnt = 0;
        while (true) {
            try {
                return onceFunction.call(param);
            } catch (Throwable t) {
                for (Class<?> throwableClass : noRetryThrowables) {
                    if (throwableClass.isInstance(t)) {
                        throw t;
                    }
                }
                if (retryCnt >= maxRetries) {
                    throw t;
                }
                t.printStackTrace();
                System.err.println("RobustExecutor retry: " + retryCnt + "/" + maxRetries);
                TimeUnit.MILLISECONDS.sleep(retryInterval);
                ++retryCnt;
            }
        }
    }
}
