package content

import io.circe.{Decoder, Encoder}
import io.circe.generic.auto._
import io.circe.jawn.decode
import io.circe.syntax.EncoderOps
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import org.apache.kafka.streams.kstream.{GlobalKTable, JoinWindows}
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.{KStream, KTable}
import org.apache.kafka.streams.scala.serialization.Serdes
import org.apache.kafka.streams.scala.ImplicitConversions._

import java.time.Duration
import java.time.temporal.ChronoUnit
import java.util.Properties

object kafkaStreams {

  private object Domain {
    type UserId = String
    type Profile = String
    type Product = String
    type OrderId = String
    type Status = String

    case class Order(orderId: OrderId, user: UserId, products: List[Product], amount: Double)

    case class Discount(profile: Profile, amount: Double)

    case class Payment(orderId: OrderId, status: Status)
  }

  private object Topics {
    val OrdersByUser = "orders-by-user"
    val DiscountProfilesByUser = "discount-profiles-by-user"
    val Discounts = "discounts"
    val Orders = "orders"
    val Payments = "payments"
    val PaidOrders = "paid-orders"
  }

  // source: emits elements
  // flow: transforms elements along the way (e.g map)
  // sink = "ingests" elements

  import Domain._
  import Topics._

  implicit def serde[A >: Null : Decoder : Encoder]: Serde[A] = {
    val serializer = (a: A) => a.asJson.noSpaces.getBytes()
    val deserializer = (bytes: Array[Byte]) => {
      val string = new String(bytes)
      decode[A](string).toOption
    }

    Serdes.fromFn[A](serializer, deserializer)
  }


  def main(args: Array[String]): Unit = {

    // topology: how the data flows through the kafka stream api
    val builder = new StreamsBuilder()

    // KStream
    val usersOrdersStream: KStream[UserId, Order] = builder.stream[UserId, Order](OrdersByUser)

    // KTable - distributed between kafka nodes
    // kafka-topics --bootstrap-server localhost:9092 --topic discount-profiles-by-user --create --config "cleanup.policy=compact"
    // topic cleanup policy set to compact => only new value with the same are kept (default policy is delete - after retention period)
    val userProfilesTable: KTable[UserId, Profile] = builder.table[UserId, Profile](DiscountProfilesByUser)

    // GlobalKTable - copied to all the nodes (available in all nodes in the cluster)
    val discountProfilesGTable: GlobalKTable[Profile, Discount] = builder.globalTable[Profile, Discount](Discounts)

    // KStream transformations: filter, map, mapValues, flatMap, flatMapValues
    val expensiveOrders: KStream[UserId, Order] = usersOrdersStream.filter { (_, order) =>
      order.amount > 1000
    }

    val listsOfProducts: KStream[UserId, List[Product]] = usersOrdersStream.mapValues { order =>
      order.products
    }

    val productsStream: KStream[UserId, Product] = usersOrdersStream.flatMapValues(_.products)

    // Joins
    val ordersWithUserProfiles: KStream[UserId, (Order, Profile)] = usersOrdersStream.join(userProfilesTable) { (order, profile) =>
      (order, profile)
    }

    val discountedOrdersStream: KStream[UserId, Order] = ordersWithUserProfiles.join(discountProfilesGTable)(
      { case (userId, (order, profile)) => profile }, // key of the join - picked from the "left" stream
      { case ((order, profile), discount) => order.copy(amount = order.amount - discount.amount) } // values of the matched records
    )

    val ordersStream: KStream[OrderId, Order] = discountedOrdersStream.selectKey((_, order) => order.orderId)

    val paymentsStream: KStream[OrderId, Payment] = builder.stream[OrderId, Payment](Payments)

    val joinWindow: JoinWindows = JoinWindows.ofTimeDifferenceWithNoGrace(Duration.of(5, ChronoUnit.MINUTES))
    val joinOrdersPayments = (order: Order, payment: Payment) => if (payment.status == "PAID") Option(order) else Option.empty[Order]
    /*val ordersPaid: KStream[OrderId, Option[Order]] = ordersStream.join(paymentsStream)(joinOrdersPayments, joinWindow)
      .filter((orderId, maybeOrder) => maybeOrder.nonEmpty)*/
    val ordersPaid: KStream[OrderId, Order] = ordersStream.join(paymentsStream)(joinOrdersPayments, joinWindow)
      .flatMapValues(maybeOrder => maybeOrder.toIterable)

    // Sink
    ordersPaid.to(PaidOrders)


    val topology = builder.build()

    val props = new Properties
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "orders-application")
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.stringSerde.getClass)

    // println(topology.describe())
    val application = new KafkaStreams(topology, props)
    application.start()
  }
}
