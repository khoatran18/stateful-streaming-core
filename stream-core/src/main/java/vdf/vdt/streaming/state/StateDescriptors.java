package vdf.vdt.streaming.state;

import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import vdf.vdt.streaming.model.schema.SourceVersionKey;
import vdf.vdt.streaming.model.schema.TableSchema;

public class StateDescriptors {

    // Broadcast State Descriptor for caching TableSchema per SourceVersionKey
    public static final MapStateDescriptor<SourceVersionKey, TableSchema> SCHEMA_BROADCAST_STATE =
            new MapStateDescriptor<>(
                    "SchemaBroadcastState",
                    TypeInformation.of(SourceVersionKey.class),
                    TypeInformation.of(TableSchema.class)
            );


}
