package org.jetlinks.supports.command;

import org.jetlinks.core.Module;
import org.jetlinks.core.ModuleInfo;
import org.jetlinks.core.command.CommandSupport;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.core.ResolvableType;
import reactor.test.StepVerifier;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * StaticCommandService 单元测试
 *
 * @author test
 */
public class StaticCommandServiceTest {

    @Mock
    private Module mockModule;

    @Mock
    private CommandSupport mockCommandSupport;

    private StaticCommandService service;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testConstructorWithThreeParams() {
        // Given
        String type = "testType";
        String name = "测试服务";
        String provider = "testProvider";

        // When
        service = new StaticCommandService(type, name, provider);

        // Then
        assertNotNull(service);
        StepVerifier.create(service.getDescription())
                .assertNext(description -> {
                    assertEquals(type, description.getType());
                    assertEquals(name, description.getName());
                    assertEquals(provider, description.getProvider());
                    assertNull(description.getDescription());
                    assertNull(description.getVersion());
                    assertNull(description.getManufacturer());
                })
                .verifyComplete();
    }

    @Test
    public void testConstructorWithFourParams() {
        // Given
        String type = "testType";
        String name = "测试服务";
        String provider = "testProvider";
        String description = "服务描述";

        // When
        service = new StaticCommandService(type, name, provider, description);

        // Then
        assertNotNull(service);
        StepVerifier.create(service.getDescription())
                .assertNext(desc -> {
                    assertEquals(type, desc.getType());
                    assertEquals(name, desc.getName());
                    assertEquals(provider, desc.getProvider());
                    assertEquals(description, desc.getDescription());
                })
                .verifyComplete();
    }

    @Test
    public void testConstructorWithFiveParams() {
        // Given
        String type = "testType";
        String name = "测试服务";
        String provider = "testProvider";
        String description = "服务描述";
        String version = "1.0.0";

        // When
        service = new StaticCommandService(type, name, provider, description, version);

        // Then
        assertNotNull(service);
        StepVerifier.create(service.getDescription())
                .assertNext(desc -> {
                    assertEquals(type, desc.getType());
                    assertEquals(name, desc.getName());
                    assertEquals(provider, desc.getProvider());
                    assertEquals(description, desc.getDescription());
                    assertEquals(version, desc.getVersion());
                })
                .verifyComplete();
    }

    @Test
    public void testConstructorWithSixParams() {
        // Given
        String type = "testType";
        String name = "测试服务";
        String provider = "testProvider";
        String description = "服务描述";
        String version = "1.0.0";
        Map<String, Object> others = new HashMap<>();
        others.put("key1", "value1");
        others.put("key2", 123);

        // When
        service = new StaticCommandService(type, name, provider, description, version, others);

        // Then
        assertNotNull(service);
        StepVerifier.create(service.getDescription())
                .assertNext(desc -> {
                    assertEquals(type, desc.getType());
                    assertEquals(name, desc.getName());
                    assertEquals(provider, desc.getProvider());
                    assertEquals(description, desc.getDescription());
                    assertEquals(version, desc.getVersion());
                    assertNotNull(desc.getMetadata());
                    assertEquals("value1", desc.getMetadata().get("key1"));
                    assertEquals(123, desc.getMetadata().get("key2"));
                })
                .verifyComplete();
    }

    @Test
    public void testConstructorWithSevenParams() {
        // Given
        String type = "testType";
        String name = "测试服务";
        String provider = "testProvider";
        String description = "服务描述";
        String version = "1.0.0";
        String manufacturer = "测试厂商";
        Map<String, Object> others = new HashMap<>();
        others.put("key1", "value1");

        // When
        service = new StaticCommandService(type, name, provider, description, version, manufacturer, others);

        // Then
        assertNotNull(service);
        StepVerifier.create(service.getDescription())
                .assertNext(desc -> {
                    assertEquals(type, desc.getType());
                    assertEquals(name, desc.getName());
                    assertEquals(provider, desc.getProvider());
                    assertEquals(description, desc.getDescription());
                    assertEquals(version, desc.getVersion());
                    assertEquals(manufacturer, desc.getManufacturer());
                    assertNotNull(desc.getMetadata());
                })
                .verifyComplete();
    }

    @Test
    public void testRegisterTemplateWithClass() {
        // Given
        service = new StaticCommandService("testType", "测试服务", "testProvider");
        when(mockModule.getId()).thenReturn("module1");

        // When
        service.registerTemplate(mockModule, TestCommandSupport.class);

        // Then
        StepVerifier.create(service.getModule("module1"))
                .assertNext(commandSupport -> {
                    assertNotNull(commandSupport);
                })
                .verifyComplete();
    }

    @Test
    public void testRegisterTemplateWithResolvableType() {
        // Given
        service = new StaticCommandService("testType", "测试服务", "testProvider");
        when(mockModule.getId()).thenReturn("module2");
        ResolvableType resolvableType = ResolvableType.forClass(TestCommandSupport.class);

        // When
        service.registerTemplate(mockModule, resolvableType);

        // Then
        StepVerifier.create(service.getModule("module2"))
                .assertNext(commandSupport -> {
                    assertNotNull(commandSupport);
                })
                .verifyComplete();
    }

    @Test
    public void testRegisterWithObject() {
        // Given
        service = new StaticCommandService("testType", "测试服务", "testProvider");
        when(mockModule.getId()).thenReturn("module3");
        TestCommandSupportImpl testSupport = new TestCommandSupportImpl();

        // When
        service.register(mockModule, testSupport);

        // Then
        StepVerifier.create(service.getModule("module3"))
                .assertNext(commandSupport -> {
                    assertNotNull(commandSupport);
                })
                .verifyComplete();
    }

    @Test
    public void testRegisterWithCommandSupport() {
        // Given
        service = new StaticCommandService("testType", "测试服务", "testProvider");
        when(mockModule.getId()).thenReturn("module4");

        // When
        service.register(mockModule, mockCommandSupport);

        // Then
        StepVerifier.create(service.getModule("module4"))
                .assertNext(commandSupport -> {
                    assertNotNull(commandSupport);
                    assertEquals(mockCommandSupport, commandSupport);
                })
                .verifyComplete();
    }

    @Test
    public void testGetDescriptionWithModules() {
        // Given
        service = new StaticCommandService("testType", "测试服务", "testProvider", "服务描述", "1.0.0");
        when(mockModule.getId()).thenReturn("module1");
        when(mockModule.getName()).thenReturn("模块1");
        service.register(mockModule, mockCommandSupport);

        // When & Then
        StepVerifier.create(service.getDescription())
                .assertNext(description -> {
                    assertEquals("testType", description.getType());
                    assertEquals("测试服务", description.getName());
                    assertEquals("testProvider", description.getProvider());
                    assertEquals("服务描述", description.getDescription());
                    assertEquals("1.0.0", description.getVersion());
                    assertNotNull(description.getModules());
                    assertEquals(1, description.getModules().size());
                    ModuleInfo moduleInfo = description.getModules().get(0);
                    assertEquals("module1", moduleInfo.getId());
                    assertEquals("模块1", moduleInfo.getName());
                })
                .verifyComplete();
    }

    @Test
    public void testGetModuleExists() {
        // Given
        service = new StaticCommandService("testType", "测试服务", "testProvider");
        when(mockModule.getId()).thenReturn("module1");
        service.register(mockModule, mockCommandSupport);

        // When & Then
        StepVerifier.create(service.getModule("module1"))
                .assertNext(commandSupport -> {
                    assertNotNull(commandSupport);
                    assertEquals(mockCommandSupport, commandSupport);
                })
                .verifyComplete();
    }

    @Test
    public void testGetModuleNotExists() {
        // Given
        service = new StaticCommandService("testType", "测试服务", "testProvider");

        // When & Then
        StepVerifier.create(service.getModule("nonExistentModule"))
                .expectComplete()
                .verify();
    }

    @Test
    public void testMultipleModules() {
        // Given
        service = new StaticCommandService("testType", "测试服务", "testProvider");
        Module module1 = mock(Module.class);
        Module module2 = mock(Module.class);
        CommandSupport support1 = mock(CommandSupport.class);
        CommandSupport support2 = mock(CommandSupport.class);

        when(module1.getId()).thenReturn("module1");
        when(module2.getId()).thenReturn("module2");

        // When
        service.register(module1, support1);
        service.register(module2, support2);

        // Then
        StepVerifier.create(service.getDescription())
                .assertNext(description -> {
                    assertNotNull(description.getModules());
                    assertEquals(2, description.getModules().size());
                })
                .verifyComplete();

        StepVerifier.create(service.getModule("module1"))
                .assertNext(cs -> assertEquals(support1, cs))
                .verifyComplete();

        StepVerifier.create(service.getModule("module2"))
                .assertNext(cs -> assertEquals(support2, cs))
                .verifyComplete();
    }

    @Test
    public void testConcurrentRegistration() throws InterruptedException {
        // Given
        service = new StaticCommandService("testType", "测试服务", "testProvider");
        int threadCount = 10;
        Thread[] threads = new Thread[threadCount];

        // When
        for (int i = 0; i < threadCount; i++) {
            final int index = i;
            threads[i] = new Thread(() -> {
                Module module = mock(Module.class);
                CommandSupport support = mock(CommandSupport.class);
                when(module.getId()).thenReturn("module" + index);
                service.register(module, support);
            });
            threads[i].start();
        }

        // Wait for all threads to complete
        for (Thread thread : threads) {
            thread.join();
        }

        // Then
        StepVerifier.create(service.getDescription())
                .assertNext(description -> {
                    assertNotNull(description.getModules());
                    assertEquals(threadCount, description.getModules().size());
                })
                .verifyComplete();
    }

    /**
     * 测试用的命令支持接口
     */
    interface TestCommandSupport {
        String testMethod();
    }

    /**
     * 测试用的命令支持实现类
     */
    static class TestCommandSupportImpl implements TestCommandSupport {
        @Override
        public String testMethod() {
            return "test";
        }
    }
}

