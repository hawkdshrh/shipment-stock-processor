package org.acme.topology;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;

import io.quarkus.kafka.client.serialization.JsonbSerde;
import java.time.Duration;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;
import javax.inject.Inject;
import org.acme.beans.Product;
import org.acme.beans.Shipment;
import org.acme.beans.SupplyUpdate;
import org.acme.beans.SupplyUpdateEntry;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;

@ApplicationScoped
public class StockShipmentTopologyProducer {

    public static final String SHIPMENTS_TOPIC = "shipments";
    public static final String UPDATED_STOCK_TOPIC = "updated-stock";
    public static final String STOCK_LEVELS_TOPIC = "stock-levels";

    private final JsonbSerde<Shipment> shipmentSerde = new JsonbSerde<>(Shipment.class);
    private final JsonbSerde<Product> productSerde = new JsonbSerde<>(Product.class);
    private final JsonbSerde<SupplyUpdate> supplyUpdateSerde = new JsonbSerde<>(SupplyUpdate.class);

    private static final Logger LOGGER = Logger.getLogger("StockShipmentTopologyProducer");

    @Inject
    @Channel("updated-stock-out")
    Emitter<SupplyUpdate> emitter;

    @Produces
    public Topology buildTopology() {
        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<String, Shipment> shipments = builder.stream(
                SHIPMENTS_TOPIC,
                Consumed.with(Serdes.String(), shipmentSerde));
        final KTable<Product, Integer> stockLevels = builder.table(
                STOCK_LEVELS_TOPIC,
                Consumed.with(productSerde, Serdes.Integer()));

        final KeyValueMapper<String, Shipment, Iterable<KeyValue<Product, Integer>>> shipmentToProductQuantitiesMapping
                = (orderId, shipment)
                -> () -> Stream.of(shipment.getShipmentLineEntries())
                        .map(e -> new KeyValue<>(e.getProduct(), e.getQuantity()))
                        .iterator();

        final KTable<Product, Integer> stockShipped = shipments.flatMap(shipmentToProductQuantitiesMapping).toTable();
        final KTable newStockLevel = stockShipped.join(stockLevels, new ProductLevelValueJoiner());
        //final KGroupedStream<Product, Integer> updatedStock = stockLevels.merge(stockShipped).groupByKey(Grouped.with(productSerde, Serdes.Integer()));

        //KTable<Product, Integer> newStockLevel = updatedStock.reduce(Integer::sum);

//        newStockLevel.toStream().foreach(new ForeachAction<Product, Integer>() {
//            @Override
//            public void apply(Product key, Integer value) {
//                SupplyUpdateEntry entry = new SupplyUpdateEntry(key, value);
//                SupplyUpdate update = new SupplyUpdate();
//                update.setSupplyCode("SHIPMENT");
//                update.setUpdateId(null);
//                update.setSupplyUpdateEntries(new SupplyUpdateEntry[]{entry});
//
//                LOGGER.log(Level.INFO, "Emitting update for {0} shares for {1}.", new Object[]{entry.getQuantity(), entry.getProduct().getProductSku()});
//
//                emitter.send(update);
//            }
//        });

        return builder.build();
    }
}
