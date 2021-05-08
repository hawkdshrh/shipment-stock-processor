package org.acme.topology;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;

import io.quarkus.kafka.client.serialization.JsonbSerde;
import io.smallrye.reactive.messaging.kafka.KafkaRecord;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;
import javax.inject.Inject;
import org.acme.beans.Product;
import org.acme.beans.Order;
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
    public static final String STOCK_LEVELS_TOPIC = "stock-levels";

    private final JsonbSerde<Order> shipmentSerde = new JsonbSerde<>(Order.class);
    private final JsonbSerde<Product> productSerde = new JsonbSerde<>(Product.class);

    private static final Logger LOGGER = Logger.getLogger("StockShipmentTopologyProducer");

    @Inject
    @Channel("stock-levels-out")
    Emitter<Integer> emitter;

    @Produces
    public Topology buildTopology() {
        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<String, Order> shipments = builder.stream(
                SHIPMENTS_TOPIC,
                Consumed.with(Serdes.String(), shipmentSerde));
        final KTable<Product, Integer> stockLevels = builder.table(
                STOCK_LEVELS_TOPIC,
                Consumed.with(productSerde, Serdes.Integer()));

        final KeyValueMapper<String, Order, Iterable<KeyValue<Product, Integer>>> shipmentToProductQuantitiesMapping
                = (orderId, shipment)
                -> () -> Stream.of(shipment.getOrderEntries())
                        .map(e -> new KeyValue<>(e.getProduct(), e.getQuantity()))
                        .iterator();

        final KStream<Product, Integer> stockShipped = shipments.flatMap(shipmentToProductQuantitiesMapping);
        
        stockShipped.foreach(new ForeachAction<Product, Integer>() {
            @Override
            public void apply(Product key, Integer value) {
                if (value != null) {

                    LOGGER.log(Level.INFO, "Shipment received for {0} shares for {1}.", new Object[]{value, key.getProductSku()});
                }
            }
        });

        final KStream<Product, Integer> newStockLevel = stockShipped.leftJoin(stockLevels,
                (leftValue, rightValue) -> (leftValue != null && rightValue != null) ? -leftValue + rightValue : null,
                Joined.with(productSerde, Serdes.Integer(), Serdes.Integer())
        );

        newStockLevel.foreach(new ForeachAction<Product, Integer>() {
            @Override
            public void apply(Product key, Integer value) {
                if (value != null) {

                    LOGGER.log(Level.INFO, "Emitting update for {0} shares for {1}.", new Object[]{value, key.getProductSku()});
                    KafkaRecord<Product, Integer> update = KafkaRecord.of(key, value);
                    emitter.send(update);
                }
            }
        });

        return builder.build();
    }

}
