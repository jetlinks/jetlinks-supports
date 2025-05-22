package org.jetlinks.supports.collector;

import lombok.AllArgsConstructor;
import org.jetlinks.core.annotation.command.CommandHandler;
import org.jetlinks.core.collector.DataCollectorProvider;
import org.jetlinks.core.collector.command.GetChannelConfigMetadataCommand;
import org.jetlinks.core.collector.command.GetCollectorConfigMetadataCommand;
import org.jetlinks.core.collector.command.GetPointConfigMetadataCommand;
import org.jetlinks.core.command.CommandMetadataResolver;
import org.jetlinks.core.metadata.PropertyMetadata;
import org.jetlinks.supports.command.AnnotationCommandSupport;
import org.springframework.core.ResolvableType;
import reactor.core.publisher.Mono;

import java.util.List;

@AllArgsConstructor
public abstract class AbstractDataCollectorProvider extends AnnotationCommandSupport implements DataCollectorProvider {
    protected final Class<?> channelConfigType;
    protected final Class<?> collectorConfigType;
    protected final Class<?> pointConfigType;


    @CommandHandler
    public Mono<List<PropertyMetadata>> getChannelConfigProperties(GetChannelConfigMetadataCommand command) {
        return Mono.just(
            CommandMetadataResolver.resolveInputs(ResolvableType.forType(channelConfigType))
        );
    }

    @CommandHandler
    public Mono<List<PropertyMetadata>> getCollectorConfigProperties(GetCollectorConfigMetadataCommand command) {
        return Mono.just(
            CommandMetadataResolver.resolveInputs(ResolvableType.forType(collectorConfigType))
        );
    }

    @CommandHandler
    public Mono<List<PropertyMetadata>> getPointConfigProperties(GetPointConfigMetadataCommand command) {
        return Mono.just(
            CommandMetadataResolver.resolveInputs(ResolvableType.forType(pointConfigType))
        );
    }
}
