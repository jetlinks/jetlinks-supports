package org.jetlinks.supports.command;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.google.common.collect.Sets;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.hibernate.validator.constraints.Range;
import org.jetlinks.core.annotation.command.CommandHandler;
import org.jetlinks.core.annotation.command.FunctionSemantics;
import org.jetlinks.core.annotation.Attr;
import org.jetlinks.core.annotation.Expands;
import org.jetlinks.core.annotation.GraphSubject;
import org.jetlinks.core.annotation.SemanticRole;
import org.jetlinks.core.annotation.SemanticTypes;
import org.jetlinks.core.annotation.ui.Presentation;
import org.jetlinks.core.metadata.FunctionKind;
import org.jetlinks.core.metadata.MetadataSemantics;
import org.jetlinks.core.metadata.PropertySemanticRole;
import org.jetlinks.core.metadata.SemanticType;
import org.jetlinks.supports.official.JetLinksDeviceMetadataCodec;
import org.jetlinks.core.command.AbstractConvertCommand;
import org.jetlinks.core.command.AbstractCommand;
import org.jetlinks.core.command.CommandSupport;
import org.jetlinks.core.metadata.DataType;
import org.jetlinks.core.metadata.types.ObjectType;
import org.jetlinks.core.metadata.types.StringType;
import org.junit.Test;
import org.springframework.context.annotation.Bean;
import org.springframework.core.ResolvableType;
import org.springframework.web.bind.annotation.RequestBody;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

public class JavaBeanCommandSupportTest {

    @Test
    public void semanticHandlerMetadataRetainsClassification() {
        JavaBeanCommandSupport support = new JavaBeanCommandSupport(new SemanticHandlers());
        support.getCommandMetadata("read")
            .as(StepVerifier::create)
            .expectNextMatches(metadata -> MetadataSemantics.functionKind(metadata).orElse(null) == FunctionKind.query)
            .verifyComplete();
    }

    @Test
    public void conflictingHandlerClassificationIsRejected() {
        StepVerifier.create(Mono.fromSupplier(() -> new JavaBeanCommandSupport(new ConflictingHandler())))
            .expectError().verify();
    }

    @Test
    public void commandClassSemanticsSurviveBothHandlerAdaptations() {
        var support = new JavaBeanCommandSupport(new ClassSemanticHandlers());
        for (String id : List.of("typed", "referenced")) {
            support.getCommandMetadata(id).as(StepVerifier::create)
                .assertNext(metadata -> {
                    org.junit.Assert.assertEquals(FunctionKind.query, MetadataSemantics.functionKind(metadata).orElseThrow());
                    org.junit.Assert.assertEquals("method", metadata.getExpand("custom").orElseThrow());
                    org.junit.Assert.assertEquals("string", metadata.getOutput().getId());
                    org.junit.Assert.assertEquals(Map.of("primary", true, "observedAt", "sampledAt"),
                                                  metadata.getExpand("presentation").orElseThrow());
                }).verifyComplete();
        }
    }

    @Test
    public void commandClassAndMethodConflictsAreRejectedForBothAdaptations() {
        for (Object handler : List.of(new TypedSemanticConflict(), new ReferencedSemanticConflict())) {
            StepVerifier.create(Mono.fromSupplier(() -> new JavaBeanCommandSupport(handler)))
                .expectError(org.hswebframework.web.exception.I18nSupportException.class).verify();
        }
    }

    @Test
    public void nestedCommandAndMethodConflictsAreRejectedForBothAdaptations() {
        for (Object handler : List.of(new TypedPresentationConflict(), new ReferencedPresentationConflict())) {
            StepVerifier.create(Mono.fromSupplier(() -> new JavaBeanCommandSupport(handler)))
                .expectError(org.hswebframework.web.exception.I18nSupportException.class).verify();
        }
    }

    @Retention(RetentionPolicy.RUNTIME)
    @Expands(key = "presentation")
    public @interface CommandPresentation {
        boolean primary();
    }

    @FunctionSemantics(FunctionKind.query)
    @Expands(@Attr(key = "custom", value = "class"))
    @CommandPresentation(primary = true)
    public static class ReadCommand extends AbstractCommand<Integer, ReadCommand> {}

    public static class ClassSemanticHandlers {
        @CommandHandler(id = "typed") @FunctionSemantics(FunctionKind.query)
        @Expands(@Attr(key = "custom", value = "method"))
        @Expands(key = "presentation", value = @Attr(key = "observedAt", value = "sampledAt"))
        public String typed(ReadCommand ignored) { return "read"; }

        @CommandHandler(value = ReadCommand.class, id = "referenced")
        @Expands(@Attr(key = "custom", value = "method"))
        @Expands(key = "presentation", value = @Attr(key = "observedAt", value = "sampledAt"))
        public String referenced() { return "read"; }
    }

    public static class TypedSemanticConflict {
        @CommandHandler @FunctionSemantics(FunctionKind.action)
        public String typed(ReadCommand ignored) { return "write"; }
    }

    public static class ReferencedSemanticConflict {
        @CommandHandler(ReadCommand.class) @FunctionSemantics(FunctionKind.action)
        public String referenced() { return "write"; }
    }

    public static class TypedPresentationConflict {
        @CommandHandler @CommandPresentation(primary = false)
        public String typed(ReadCommand ignored) { return "read"; }
    }

    public static class ReferencedPresentationConflict {
        @CommandHandler(ReadCommand.class) @CommandPresentation(primary = false)
        public String referenced() { return "read"; }
    }

    @Test
    public void semanticAnnotationsSurviveTheOriginalMetadataCodec() {
        var codec = JetLinksDeviceMetadataCodec.getInstance();
        new JavaBeanCommandSupport(new SemanticHandlers()).getCommandMetadata("snapshot")
            .map(metadata -> codec.doDecode(JSON.toJSONString(Map.of("functions", List.of(metadata.toJson())))))
            .flatMap(codec::encode).flatMap(codec::decode)
            .as(StepVerifier::create).assertNext(metadata -> {
                var function = metadata.getFunctions().get(0);
                org.junit.Assert.assertEquals(FunctionKind.query, MetadataSemantics.functionKind(function).orElse(null));
                var output = (ObjectType) function.getOutput();
                org.junit.Assert.assertEquals(true, output.getProperty("subject").orElseThrow().getExpands().get("graphSubject"));
                org.junit.Assert.assertFalse(output.getProperty("subject").orElseThrow().getExpands().containsKey("semanticRole"));
                org.junit.Assert.assertEquals("identity", output.getProperty("id").orElseThrow().getExpands().get("semanticRole"));
                org.junit.Assert.assertEquals("futureRole", output.getProperty("custom").orElseThrow().getExpands().get("semanticRole"));
                org.junit.Assert.assertEquals("sampled", ((Map<?, ?>) output.getProperty("value").orElseThrow()
                    .getExpands().get("presentation")).get("observedAt"));
                MetadataSemantics.validateOutput(output);
            }).verifyComplete();
    }

    public static class SemanticHandlers {
        @CommandHandler
        @FunctionSemantics(FunctionKind.query)
        public int read() { return 1; }

        @CommandHandler
        @FunctionSemantics(FunctionKind.query)
        public SemanticSnapshot snapshot() { return new SemanticSnapshot(); }
    }

    @Getter
    @Setter
    @SemanticTypes({SemanticType.observation, SemanticType.result})
    public static class SemanticSnapshot {
        @Schema(title = "Record") @SemanticRole(PropertySemanticRole.identity)
        private String id;
        @Schema(title = "Subject") @GraphSubject
        private String subject;
        @Schema(title = "Sampled") @SemanticRole(PropertySemanticRole.observedAt)
        private long sampled;
        @Schema(title = "Value") @Presentation(primary = true, observedAt = "sampled")
        private double value;
        @Schema(title = "Custom") @Expands(@Attr(key = "semanticRole", value = "futureRole"))
        private String custom;
    }

    public static class ConflictingHandler {
        @CommandHandler(expands = @Attr(key = "functionKind", value = "action"))
        @FunctionSemantics(FunctionKind.query)
        public int read() { return 1; }
    }


    public static final String TEST_COMMAND_NAME = "测试命令";
    public static final String TEST_COMMAND_DESCRIPTION = "测试命令描述";

    @Test
    public void testVoid() {

        MyBean bean = new MyBean();

        JavaBeanCommandSupport support = new JavaBeanCommandSupport(
            bean,
            Sets.newHashSet("callVoid"));

        support.getCommandMetadata("callVoid")
               .doOnNext(System.out::println)
               .as(StepVerifier::create)
               .expectNextCount(1)
               .verifyComplete();

        support.executeToMono("callVoid", Collections.emptyMap())
               .as(StepVerifier::create)
               .expectComplete()
               .verify();
    }

    @Test
    public void testCallSingleArg() {

        MyBean bean = new MyBean();

        JavaBeanCommandSupport support = new JavaBeanCommandSupport(
            bean,
            Sets.newHashSet("callSingleArg"));

        support.getCommandMetadata("callSingleArg")
               .as(StepVerifier::create)
               .expectNextCount(1)
               .verifyComplete();

        support.executeToMono("callSingleArg", Collections.singletonMap("val", "123"))
               .as(StepVerifier::create)
               .expectNext(123)
               .verifyComplete();
    }

    @Test
    public void testCallMultiArg() {

        MyBean bean = new MyBean();

        JavaBeanCommandSupport support = new JavaBeanCommandSupport(
            bean,
            Sets.newHashSet("callSingleArg"));

        support.getCommandMetadata("callSingleArg")
               .doOnNext(cmd -> {
                   System.out.println(JSON.toJSONString(cmd.toJson(), SerializerFeature.PrettyFormat));
               })
               .as(StepVerifier::create)
               .expectNextCount(1)
               .verifyComplete();

        support.executeToMono("callSingleArg", Collections.singletonMap("val", "123"))
               .as(StepVerifier::create)
               .expectNext(123)
               .verifyComplete();

        support.executeToMono("callSingleArg", Collections.singletonMap("val", null))
               .as(StepVerifier::create)
               .expectNext(0)
               .verifyComplete();
    }

    @Test
    public void testCallCommand() {
        MyBean bean = new MyBean();

        JavaBeanCommandSupport support = new JavaBeanCommandSupport(
            bean,
            Sets.newHashSet("callCommand"));

        support.getCommandMetadata("Test")
               .doOnNext(System.out::println)
               .as(StepVerifier::create)
               .expectNextMatches(f -> f.getName().equals(TEST_COMMAND_NAME)
                   && f.getDescription().equals(TEST_COMMAND_DESCRIPTION))
               .verifyComplete();

        support.executeToMono("Test", Collections.singletonMap("val", "123"))
               .as(StepVerifier::create)
               .expectNext(1)
               .verifyComplete();
    }

    @Test
    public void testIgnore() {
        MyBean bean = new MyBean();

        JavaBeanCommandSupport support = new JavaBeanCommandSupport(
            bean,
            Sets.newHashSet("ignore"));

        support.getCommandMetadata("ignore")
               .as(StepVerifier::create)
               .expectNextCount(0)
               .verifyComplete();

    }


    @Test
    public void testBody() {
        MyBean bean = new MyBean();

        JavaBeanCommandSupport support = new JavaBeanCommandSupport(
            bean,
            Sets.newHashSet("callBody"));

        support.getCommandMetadata("callBody")
               .doOnNext(System.out::println)
               .as(StepVerifier::create)
               .expectNextCount(1)
               .verifyComplete();

        support.executeToMono("callBody", Collections.singletonMap("data", "123"))
               .doOnNext(System.out::println)
               .as(StepVerifier::create)
               .expectNextCount(1)
               .verifyComplete();
    }

    @Test
    public void testCustomOutput() {
        MyBean bean = new MyBean();

        JavaBeanCommandSupport support = new JavaBeanCommandSupport(
            bean,
            Sets.newHashSet("custom"));

        support
            .getCommandMetadata("custom")
            .as(StepVerifier::create)
            .expectNextMatches(metadata -> metadata.getOutput() instanceof StringType)
            .verifyComplete();

        support
            .getCommandMetadata("custom", Collections.singletonMap("deviceId", "test"))
//            .doOnNext(metadata -> System.out.println(JSON.toJSONString(metadata.toJson(), SerializerFeature.PrettyFormat)))
            .as(StepVerifier::create)
            .expectNextMatches(metadata -> metadata.getOutput() instanceof ObjectType)
            .verifyComplete();

    }

    @Test
    public void testTemplate() {

        // 使用接口定义模版
        JavaBeanCommandSupport template =
            JavaBeanCommandSupport.createTemplate(ResolvableType.forClassWithGenerics(MyBeanApi.class, TestBody.class));

        template.getCommandMetadata()
                .subscribe(System.out::println);

        // 创建实现
        CommandSupport copy = template.copyWith(new MyBean() {
            @Override
            public TestBody callBody(TestBody data) {
                data.data = 234;
                return super.callBody(data);
            }
        });
        // 执行
        copy
            .executeToMono("callBody", Collections.singletonMap("data", "123"))
            .as(StepVerifier::create)
            .expectNextCount(1)
            .verifyComplete();


    }

    public static class MyBean implements MyBeanApi<TestBody> {

        public String callMultiArg(int val, String val2) {
            System.out.println("callMultiArg(" + val + "," + val2 + ")");
            return val + val2;
        }

        public int callSingleArg(@Range(min = 10, max = 100) int val) {
            System.out.println("callSingleArg(" + val + ")");
            return val;
        }


        @CommandHandler
        public int callCommand(TestCommand<String> cmd) {
            System.out.println("callCommand(" + cmd + ")");
            System.out.println(cmd.readable());
            return 1;
        }

        @CommandHandler(ignore = true)
        public int ignore(int val) {
            return val;
        }

        public void callVoid() {
            System.out.println("callVoid");
        }

        @CommandHandler(outputProvider = "customOutput")
        public String custom(String deviceId) {
            return deviceId;
        }

        private Mono<DataType> customOutput(String deviceId) {
            System.out.println("customOutput(" + deviceId + ")");
            return Mono.just(
                new ObjectType()
                    .addProperty(deviceId, StringType.GLOBAL)
            );
        }

    }

    public interface MyBeanApi<T> {

        @CommandHandler
        default T callBody(@RequestBody T data) {
            System.out.println("callBody(" + data + ")");
            return data;
        }

    }

    @Getter
    @Setter
    @ToString
    public static class TestBody {
        @Schema(title = "Data")
        private int data;
    }

    @Schema(title = TEST_COMMAND_NAME, description = TEST_COMMAND_DESCRIPTION)
    public static class TestCommand<T> extends AbstractConvertCommand<Flux<T>, TestCommand<T>> {

    }
}
