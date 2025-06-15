package edu.misis.runner

import org.springframework.context.annotation.Configuration
import org.springframework.statemachine.action.Action
import org.springframework.statemachine.config.EnableStateMachine
import org.springframework.statemachine.config.EnumStateMachineConfigurerAdapter
import org.springframework.statemachine.config.builders.StateMachineStateConfigurer
import org.springframework.statemachine.config.builders.StateMachineTransitionConfigurer

enum class StreamState {
    INITIAL,
    INITIALIZING_BUCKET,
    BUCKET_INITIALIZED,
    IN_PROGRESS,
    AWAIT_TERMINATION,
    TERMINATED,
}

enum class StreamEvent {
    INITIALIZE_BUCKET,
    BUCKET_INITIALIZED,
    START_STREAM,
    STOP_STREAM,
    STREAM_TERMINATED,
}

@Configuration
@EnableStateMachine
class StreamStateMachineConfig : EnumStateMachineConfigurerAdapter<StreamState, StreamEvent>() {
    override fun configure(states: StateMachineStateConfigurer<StreamState, StreamEvent>) {
        states.withStates()
            .initial(StreamState.INITIAL)
            .end(StreamState.TERMINATED)
            .states(StreamState.entries.toSet())
    }

    override fun configure(
        transitions: StateMachineTransitionConfigurer<StreamState, StreamEvent>
    ) {
        transitions
            .withExternal()
                .source(StreamState.INITIAL)
                .target(StreamState.BUCKET_INITIALIZED)
                .event(StreamEvent.BUCKET_INITIALIZED)
                .and()
            .withExternal()
                .source(StreamState.BUCKET_INITIALIZED)
                .target(StreamState.IN_PROGRESS)
                .event(StreamEvent.START_STREAM)
                .and()
            .withExternal()
                .source(StreamState.AWAIT_TERMINATION)
                .target(StreamState.TERMINATED)
                .event(StreamEvent.STREAM_TERMINATED)

        val cancelableStates = setOf(
            StreamState.INITIAL,
            StreamState.BUCKET_INITIALIZED,
            StreamState.IN_PROGRESS,
        )
        cancelableStates.forEach {
            transitions.withExternal()
                .source(it)
                .target(StreamState.AWAIT_TERMINATION)
                .event(StreamEvent.STOP_STREAM)
        }
    }

    fun initializeBucket() = Action<StreamState, StreamEvent> {

    }
}