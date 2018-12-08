package io.getquill.quotation

import io.getquill._
import io.getquill.dsl.DynamicQueryDsl

class DynamicQuerySpec extends Spec {

  object testContext extends MirrorContext(MirrorIdiom, Literal) with TestEntities with DynamicQueryDsl
  import testContext._

  def test[T: QueryMeta](d: Quoted[Query[T]], s: Quoted[Query[T]]) =
    testContext.run(d).string mustEqual testContext.run(s).string

  "dynamicQuery" in {
    test(
      dynamicQuery[TestEntity],
      query[TestEntity])
  }

  "dynamicQuerySchema" - {
    "no aliases" in {
      test(
        dynamicQuerySchema[TestEntity]("test"),
        querySchema[TestEntity]("test"))
    }
    "one alias" in {
      test(
        dynamicQuerySchema[TestEntity]("test", alias(_.i, "ii")),
        querySchema[TestEntity]("test", _.i -> "ii"))
    }
    "multiple aliases" in {
      test(
        dynamicQuerySchema[TestEntity]("test", alias(_.i, "ii"), alias(_.s, "ss")),
        querySchema[TestEntity]("test", _.i -> "ii", _.s -> "ss"))
    }
    "dynamic alias list" in {
      val aliases = List[DynamicAlias[TestEntity]](alias(_.i, "ii"), alias(_.s, "ss"))
      test(
        dynamicQuerySchema[TestEntity]("test", aliases: _*),
        querySchema[TestEntity]("test", _.i -> "ii", _.s -> "ss"))
    }
    "path property" in {
      case class S(v: String) extends Embedded
      case class E(s: S)
      test(
        dynamicQuerySchema[E]("e", alias(_.s.v, "sv")),
        querySchema[E]("e", _.s.v -> "sv"))
    }
  }

  "map" - {
    "simple" in {
      test(
        dynamicQuery[TestEntity].map(v => v.i),
        query[TestEntity].map(v => v.i))
    }
    "dynamic" in {
      var cond = true
      test(
        dynamicQuery[TestEntity].map(v => if (cond) v.i else 1),
        query[TestEntity].map(v => v.i))

      cond = false
      test(
        dynamicQuery[TestEntity].map(v => if (cond) v.i else 1),
        query[TestEntity].map(v => 1))
    }
  }

  "flatMap" - {
    "simple" in {
      test(
        dynamicQuery[TestEntity].flatMap(v => dynamicQuery[TestEntity]),
        query[TestEntity].flatMap(v => query[TestEntity]))
    }
    "mixed with static" in {
      test(
        dynamicQuery[TestEntity].flatMap(v => query[TestEntity]),
        query[TestEntity].flatMap(v => query[TestEntity]))

      test(
        query[TestEntity].flatMap(v => dynamicQuery[TestEntity]),
        query[TestEntity].flatMap(v => query[TestEntity]))
    }
    "with map" in {
//      val q = quote {
//        query[TestEntity].flatMap(v => query[TestEntity].map(v1 => (v, v1)))
//      }
      val q2 = dynamicQuery[TestEntity].flatMap(v => dynamicQuery[TestEntity].map(v1 => (v, v1)))
      q2.toString mustEqual null
      test(
        dynamicQuery[TestEntity].flatMap(v => dynamicQuery[TestEntity].map(v1 => (v, v1))),
        query[TestEntity].flatMap(v => query[TestEntity].map(v1 => (v, v1))))
    }
  }

  "filter" in {

  }
}