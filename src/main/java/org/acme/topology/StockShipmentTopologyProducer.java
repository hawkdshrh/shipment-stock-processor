package org.acme.topology;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;

import io.quarkus.kafka.client.serialization.JsonbSerde;
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
import org.apache.kafka.streams.kstream.Joined;
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

        final KeyValueMapper<String, SupplyUpdate, Iterable<KeyValue<Product, Integer>>> supplyUpdateToProductQuantitiesMapping
                = (supplyUpdateId, supplyUpdate)
                -> () -> Stream.of(supplyUpdate.getSupplyUpdateEntries())
                        .map(e -> new KeyValue<>(e.getProduct(), e.getQuantity()))
                        .iterator();

        final KStream<Product, Integer> stockShipped = shipments.flatMap(shipmentToProductQuantitiesMapping);
//        final KStream<Product, Integer> latestStockLevels = stockUpdates.flatMap(supplyUpdateToProductQuantitiesMapping);
//        final KGroupedStream<Product, Integer> updatedStock = latestStockLevels.merge(stockShipped).groupByKey(Grouped.with(productSerde, Serdes.Integer()));

        final KStream<Product, Integer> newStockLevel = stockShipped.leftJoin(stockLevels,
                (leftValue, rightValue) -> (leftValue != null && rightValue != null) ? - leftValue + rightValue : null,
                Joined.with(productSerde, Serdes.Integer(), Serdes.Integer())
        );


        newStockLevel.foreach(new ForeachAction<Product, Integer>() {
            @Override
            public void apply(Product key, Integer value) {
                if (value != null) {
                    SupplyUpdateEntry entry = new SupplyUpdateEntry(key, value);
                    SupplyUpdate update = new SupplyUpdate();
                    update.setSupplyCode("SHIPMENT");
                    update.setUpdateId(null);
                    update.setSupplyUpdateEntries(new SupplyUpdateEntry[]{entry});

                    LOGGER.log(Level.INFO, "Emitting update for {0} shares for {1}.", new Object[]{entry.getQuantity(), entry.getProduct().getProductSku()});

                    emitter.send(update);
                }
            }
        });

        return builder.build();
    }

}
