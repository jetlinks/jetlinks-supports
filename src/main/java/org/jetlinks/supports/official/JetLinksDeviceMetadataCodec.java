package org.jetlinks.supports.official;

import com.alibaba.fastjson.JSON;
import org.jetlinks.core.metadata.DeviceMetadata;
import org.jetlinks.core.metadata.DeviceMetadataCodec;
import reactor.core.publisher.Mono;

/*
 {
  "id": "test",
  "name": "测试",
  "properties": [
    {
      "id": "name",
      "name": "名称",
      "valueType": {
        "type": "string"
      }
    }
  ],
  "functions": [
    {
      "id": "playVoice",
      "name": "播放声音",
      "inputs": [
        {
          "id": "text",
          "name": "文字内容",
          "valueType": {
            "type": "string"
          }
        }
      ],
      "output": {
        "type": "boolean"
      }
    }
  ],
  "events": [
    {
      "id": "temp_sensor",
      "name": "温度传感器",
      "valueType": {
        "type": "double"
      }
    },
    {
      "id": "fire_alarm",
      "name": "火警",
      "valueType": {
        "type": "object",
        "properties": [
          {
            "id": "location",
            "name": "地点",
            "valueType": {
              "type": "string"
            }
          },
          {
            "id": "lng",
            "name": "经度",
            "valueType": {
              "type": "double"
            }
          },
          {
            "id": "lat",
            "name": "纬度",
            "valueType": {
              "type": "double"
            }
          }
        ]
      }
    }
  ]
}
 */

/**
 * @author zhouhao
 * @since 1.0.0
 */
public class JetLinksDeviceMetadataCodec implements DeviceMetadataCodec {

    @Override
    public String getId() {
        return "jetlinks";
    }

    @Override
    public Mono<DeviceMetadata> decode(String source) {
        return Mono.just(new JetLinksDeviceMetadata(JSON.parseObject(source)));
    }

    @Override
    public Mono<String> encode(DeviceMetadata metadata) {
        return Mono.just(new JetLinksDeviceMetadata(metadata).toJson().toJSONString());
    }
}
