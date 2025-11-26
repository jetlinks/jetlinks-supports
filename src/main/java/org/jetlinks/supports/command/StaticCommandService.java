package org.jetlinks.supports.command;

import org.jetlinks.core.Module;
import org.jetlinks.core.ModuleInfo;
import org.jetlinks.core.command.CommandSupport;
import org.jetlinks.core.command.service.CommandService;
import org.jetlinks.core.command.service.CommandServiceSupport;
import org.jetlinks.core.command.service.ServiceDescription;
import org.jetlinks.core.utils.MetadataUtils;
import org.springframework.core.ResolvableType;
import reactor.core.publisher.Mono;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 *
 * 静态命令服务
 * <pre>{@code
 *
 *   StaticCommandService service =
 *   new StaticCommandService("parkingSystem","停车系统","jetlinks");
 *
 *   service.registerTemplate(Modules.lot,LotService.class);
 *   service.registerTemplate(Modules.lot,RecordService.class);
 *
 *
 * }</pre>
 *
 * @author zhouhao
 * @since 1.3.2
 *
 */
public class StaticCommandService extends AnnotationCommandSupport implements CommandService, CommandServiceSupport {

    private final Map<String, ModuleRef> modules = new LinkedHashMap<>();

    private final String type;
    private final String name;
    private final String provider;
    private final String description;
    private final String version;
    private final String manufacturer;
    private final Map<String, Object> others;

    public StaticCommandService(String type, String name, String provider) {
        this(type, name, provider, null, null, null);
    }

    public StaticCommandService(String type, String name, String provider, String description) {
        this(type, name, provider, description, null, null, null);
    }

    public StaticCommandService(String type, String name, String provider, String description, String version) {
        this(type, name, provider, description, version, null, null);
    }

    public StaticCommandService(String type, String name, String provider, String description, String version, Map<String, Object> others) {
        this(type, name, provider, description, version, null, others);
    }

    public StaticCommandService(String type, String name, String provider, String description, String version, String manufacturer, Map<String, Object> others) {
        this.type = type;
        this.name = name;
        this.provider = provider;
        this.description = description;
        this.version = version;
        this.manufacturer = manufacturer;
        this.others = others;
    }

    protected Module wrapModule(Module origin, Class<?> type) {
        Map<String, Object> metadata = origin.getMetadata() == null ? new HashMap<>() : new HashMap<>(origin.getMetadata());
        MetadataUtils.parseExpands(type).forEach(metadata::putIfAbsent);

        ModuleInfo info = ModuleInfo.of(origin);
        info.setMetadata(Collections.unmodifiableMap(metadata));
        return info;

    }

    public void registerTemplate(Module module, Class<?> commandSupport) {
        synchronized (this) {
            modules.put(module.getId(), new ModuleRef(wrapModule(module, commandSupport),
                                                      JavaBeanCommandSupport.createTemplate(commandSupport)));
        }
    }

    public void registerTemplate(Module module, ResolvableType type) {
        synchronized (this) {
            modules.put(module.getId(), new ModuleRef(wrapModule(module, type.toClass()), JavaBeanCommandSupport.createTemplate(type)));
        }
    }

    public void register(Module module, Object commandSupport) {
        synchronized (this) {
            modules.put(module.getId(), new ModuleRef(wrapModule(module, commandSupport.getClass()), new JavaBeanCommandSupport(commandSupport)));
        }
    }

    public void register(Module module, CommandSupport commandSupport) {
        synchronized (this) {
            modules.put(module.getId(), new ModuleRef(wrapModule(module, commandSupport.getClass()), commandSupport));
        }
    }

    @Override
    public Mono<ServiceDescription> getDescription() {
        ServiceDescription description = new ServiceDescription();
        description.setProvider(provider);
        description.setType(type);
        description.setMetadata(others);
        description.setVersion(version);
        description.setName(name);
        description.setDescription(this.description);
        description.setManufacturer(this.manufacturer);
        description.setModules(
            modules
                .values()
                .stream()
                .map(ref -> ModuleInfo.of(ref.module))
                .toList());
        return Mono.just(description);
    }

    @Override
    public Mono<CommandSupport> getModule(String module) {
        return Mono.fromSupplier(() -> {
            ModuleRef ref = modules.get(module);
            return ref != null ? ref.command : null;
        });
    }

    record ModuleRef(Module module, CommandSupport command) {
    }
}
