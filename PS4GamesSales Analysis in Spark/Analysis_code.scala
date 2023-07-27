import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object project_2 {
  val sc = new SparkContext("local", "project_name")
  def main(args: Array[String]): Unit = {


    val data = sc.textFile("DataSet path")

    val processedRDD = data.filter(row => {
      val columns = row.split(",") // Assuming CSV format with comma-separated values
      !columns.exists(col => col == null || col.trim.isEmpty || col.trim == "N/A")
    })

    val x = processedRDD.collect().length
    println(x)

    top10_gms_glb_sales(processedRDD) // 1. finding top 10 games sold across the world
    TotalsalesbyRegion_max_min_rgns(processedRDD) // 2. total sales by region and finding out the max and min sales across the regions
    yearlySales(processedRDD) // 3. total sales in a particular year
    avg_sls_top5Pub_rgns(processedRDD) // 4. average sales of top 5 publishers across each region
    numGenres_yrly_ByPub(processedRDD) // 5. max number of genres published by different publishers in different years
    top5_bst_genretoRegion(processedRDD) // 6. top 5 best genres across different regions
    market_share_byPub(processedRDD) // 7. market share by top 20 publishers across the world
    market_share_genrePercentage(processedRDD) // 8. market share by genres across the world
    SalesByGenre(processedRDD) // 9. total number of sales by top 5 genres
    salesbytop10Publisher(processedRDD) // 10. total number of sales by top 10 publishers
    sls_diff_rgns_acc_genre(processedRDD) // 11. sales in different regions according to genre
  }

  def top10_gms_glb_sales(data: RDD[String]): Unit = {
    // Remove the header row from RDD
    val header = data.first()
    val salesRDD = data.filter(row => row != header)

    // Create a pair RDD with (game, global sales)
    val gameSalesRDD = salesRDD.map(row => {
      val columns = row.split(",")
      val game = columns(0)
      val globalSales = columns(8).toDouble
      (game, globalSales)
    })

    // Calculate the total sales for each game
    val totalSalesRDD = gameSalesRDD.reduceByKey(_ + _)

    // Sort the RDD by total sales in descending order
    val topSellingGamesRDD = totalSalesRDD.sortBy(_._2, ascending = false)

    // Display the top-selling games
    topSellingGamesRDD.take(10).foreach(println)
  }

  def TotalsalesbyRegion_max_min_rgns(data_2: RDD[String]): Unit = {
    // Remove the header row from RDD
    val header = data_2.first()
    val filteredRDD = data_2.filter(row => row != header).map(x => x.split(","))

    val salesRDD = filteredRDD.map(row => (
      row(4).toDouble, // North America sales
      row(5).toDouble, // Europe sales
      row(6).toDouble, // Japan sales
      row(7).toDouble, // Rest of World sales
      row(8).toDouble // Global sales
    ))

    val totalSalesByRegionRDD = salesRDD.reduce { case ((na1, eu1, jp1, row1, global1), (na2, eu2, jp2, row2, global2)) =>
      (na1 + na2, eu1 + eu2, jp1 + jp2, row1 + row2, global1 + global2)
    }

    val regionWithHighestSales = Seq("North America", "Europe", "Japan", "Rest of World", "Global")
    val HighestSalesRegion = regionWithHighestSales.maxBy(region => totalSalesByRegionRDD.productElement(regionWithHighestSales
      .indexOf(region)).asInstanceOf[Double])
    val regionWithLowestSales = Seq("North America", "Europe", "Japan", "Rest of World", "Global")
    val LowestSalesRegion = regionWithLowestSales.minBy(region => totalSalesByRegionRDD.productElement(regionWithLowestSales
      .indexOf(region)).asInstanceOf[Double])
    Seq("North America", "Europe", "Japan", "Rest of World", "Global").foreach { region =>
      val totalSales = totalSalesByRegionRDD.productElement(regionWithHighestSales.indexOf(region)).asInstanceOf[Double]
      println(s"=>  $region - $totalSales")
    }
    println(s"Total sales per region: $totalSalesByRegionRDD")
    println(s"Highest sales region: $HighestSalesRegion")
    println(s"Lowest sales region: $LowestSalesRegion")
  }

  def yearlySales(data_3: RDD[String]): Unit = {
    val header = data_3.first()
    val filteredRDD = data_3.filter(row => row != header)
    val salesByYearRDD = filteredRDD.map(row => (row.split(",")(1), row.split(",")(8).toDouble))
    val totalSalesByYearRDD = salesByYearRDD.reduceByKey(_ + _)
    val sortedSalesByYearRDD = totalSalesByYearRDD.sortByKey()

    sortedSalesByYearRDD.foreach { case (year, totalSales) => println(s"Year: $year, Total Sales: $totalSales") }
  }

  def avg_sls_top5Pub_rgns(data_8: RDD[String]): Unit = {

    val filteredRDD2 = data_8.zipWithIndex().filter {
      case (row, index) =>
        index > 0
    }.map(_._1)
    //filteredRDD2.foreach(println)

    val salesByPubRDD = filteredRDD2.map(row => (row.split(",")(3), (
      row.split(",")(4).toDouble,
      row.split(",")(5).toDouble,
      row.split(",")(6).toDouble,
      row.split(",")(7).toDouble,
      row.split(",")(8).toDouble
    )))

    val totalSalesByPubRDD = salesByPubRDD.reduceByKey((sales1, sales2) => (
      sales1._1 + sales2._1,
      sales1._2 + sales2._2,
      sales1._3 + sales2._3,
      sales1._4 + sales2._4,
      sales1._5 + sales2._5
    ))

    val topPubCount = 5
    val topPubRDD = totalSalesByPubRDD.top(topPubCount)(Ordering.by(_._2._5))

    val avgSalesRegionPubRDD = topPubRDD.flatMap {
      case (publisher, (naSales, euSales, jpSales, rowSales, globalSales)) =>
        Seq(
          ("North America", publisher, naSales / topPubCount),
          ("Europe", publisher, euSales / topPubCount),
          ("Japan", publisher, jpSales / topPubCount),
          ("Rest of the World", publisher, rowSales / topPubCount),
          ("Global", publisher, globalSales / topPubCount)
        )
    }

    avgSalesRegionPubRDD.foreach {
      case (region, publisher, averagesales) =>
        println(s"Region: $region, Publisher: $publisher, Average Sales: $averagesales")
    }
  }

  def numGenres_yrly_ByPub(data_10: RDD[String]): Unit = {

    val header = data_10.first()
    val filteredRDD = data_10.filter(row => row != header)

    val genresByYearAndPubRDD = filteredRDD.map(row => {
      val year = row.split(",")(1)
      val publisher = row.split(",")(3)
      val genre = row.split(",")(2)
      ((year, publisher), genre)
    }).groupByKey().mapValues(_.toSet.size)

    val sortedGenresByYearAndPubRDD = genresByYearAndPubRDD.sortBy(_._2, ascending = false)
    val top20Pub = sortedGenresByYearAndPubRDD.take(30)

    top20Pub.foreach {
      case ((year, publisher), genreCount) =>
        println(s"Year: $year, Publisher: $publisher, Number of Genres: $genreCount")
    }
  }


  def sls_diff_rgns_acc_genre(data_6: RDD[String]): Unit = {

    val filteredRDD2 = data_6.zipWithIndex().filter {
      case (row, index) => index > 0
    }.map(a => ((a._1.split(","))))
      .map { case (a) => (a(2), (a(4).toDouble, a(5).toDouble, a(6).toDouble, a(7).toDouble, a(8).toDouble)) }
      .groupByKey().collect()
      .map(a => (a._1, sc.parallelize(a._2.toList).collect()
        .reduce((a, b) => (a._1 + b._1, a._2 + b._2, a._3 + b._3, a._4 + b._4, a._5 + b._5))))

    //filteredRDD2.foreach(x=>println(x._1+" "+x._2+" "))
    filteredRDD2.foreach { case (genre, sales) =>
      println(genre)
      println("NA: " + sales._1)
      println("EU: " + sales._2)
      println("JP: " + sales._3)
      println("Other: " + sales._4)
      println("Global: " + sales._5)
      println()
    }
  }

  def top5_bst_genretoRegion(data_7: RDD[String]): Unit = {

    val filteredRDD2 = data_7.zipWithIndex().filter {
      case (row, index) =>
        index > 0
    }.map(_._1)

    val salesByGenreRDD = filteredRDD2.map(row => (row.split(",")(2), (
      row.split(",")(4).toDouble,
      row.split(",")(5).toDouble,
      row.split(",")(6).toDouble,
      row.split(",")(7).toDouble,
      row.split(",")(8).toDouble
    )))

    val totalSalesByGenreRDD = salesByGenreRDD.reduceByKey((sales1, sales2) => (
      sales1._1 + sales2._1,
      sales1._2 + sales2._2,
      sales1._3 + sales2._3,
      sales1._4 + sales2._4,
      sales1._5 + sales2._5
    ))

    val topGenresByRegionRDD = totalSalesByGenreRDD.flatMap { case (genre, (naSales, euSales, jpSales, rowSales, globalSales)) =>
      Seq(
        ("North America", genre, naSales),
        ("Europe", genre, euSales),
        ("Japan", genre, jpSales),
        ("Rest of World", genre, rowSales),
        ("Global", genre, globalSales)
      )
    }.groupBy(_._1).flatMapValues(_.toList.sortBy(-_._3).take(5)).map { case (_, (region, genre, sales)) =>
      (region, genre, sales)
    }

    topGenresByRegionRDD.foreach { case (region, genre, sales) =>
      println(s"Region: $region, Genre: $genre, Sales: $sales")
    }
  }

  def market_share_byPub(data_9: RDD[String]): Unit = {

    val filteredRDD2 = data_9.zipWithIndex().filter {
      case (row, index) =>
        index > 0
    }.map(_._1)

    val totalGlobalSalesByPubRDD = filteredRDD2.map(row => {
      val publisher = row.split(",")(3)
      val globalSales = row.split(",")(8).toDouble
      (publisher, globalSales)
    }).reduceByKey(_ + _)

    val totalGlobalSales = totalGlobalSalesByPubRDD.map(_._2).sum()
    val pubGlobalSalesperctRDD = totalGlobalSalesByPubRDD.mapValues(globalSales => (globalSales / totalGlobalSales) * 100)
    val topPubBySales = pubGlobalSalesperctRDD.sortBy(-_._2)

    println("Top 20 Publishers by Global sales:")
    topPubBySales.take(20).foreach {
      case (publisher, salesPercentage) =>
        println(s"$publisher: $salesPercentage")
    }
  }


  def SalesByGenre(data_4: RDD[String]): Unit = {

    val header = data_4.first()
    val filteredRDD = data_4.filter(row => row != header)

    val salesByGenreRDD = filteredRDD.map(row => (row.split(",")(2), row.split(",")(8).toDouble))
    val totalSalesByGenreRDD = salesByGenreRDD.reduceByKey(_ + _)
    val sortedSalesByGenreRDD = totalSalesByGenreRDD.sortBy(-_._2)

    sortedSalesByGenreRDD.foreach { case (genre, totalSales) => println(s"Genre: $genre, Total Sales: $totalSales") }
    val mostPopularGenre = sortedSalesByGenreRDD.first()._1
    println(s"Most Popular Game Genre: $mostPopularGenre")
    val top5GamesRDD = sortedSalesByGenreRDD.take(5)
    top5GamesRDD.foreach { case (game, totalSales) => println(s"Game: $game, Total Sales: $totalSales") }
  }

  def salesbytop10Publisher(data_5: RDD[String]): Unit = {

    val header = data_5.first()
    val filteredRDD = data_5.filter(row => row != header)

    val salesByPublisherRDD = filteredRDD.map(row => (row.split(",")(3), row.split(",")(8).toDouble))
    val totalSalesByPublisherRDD = salesByPublisherRDD.reduceByKey(_ + _)
    val sortedSalesByPublisherRDD = totalSalesByPublisherRDD.sortBy(-_._2)
    val top10PublishersRDD = sortedSalesByPublisherRDD.take(10)

    top10PublishersRDD.foreach { case (publisher, totalSales) =>
      println(s"Top Publisher: $publisher, Total Sales: $totalSales")
    }
  }

  def market_share_genrePercentage(data_11: RDD[String]): Unit = {

    val header = data_11.first()
    val filteredRDD = data_11.filter(row => row != header)

    val totalGlobalSalesbyGenreRDD = filteredRDD.map(row => {
      val genre = row.split(",")(2)
      val globalSales = row.split(",")(8).toDouble
      (genre, globalSales)
    }).reduceByKey(_ + _)

    val totalGlobalSales = totalGlobalSalesbyGenreRDD.map(_._2).sum()

    val genreGlobalSalesPerctRDD = totalGlobalSalesbyGenreRDD.mapValues(globalSales => (globalSales / totalGlobalSales) * 100)
    val topGenreBySales = genreGlobalSalesPerctRDD.sortBy(-_._2)

    topGenreBySales.foreach {
      case (genre, salesPercentage) =>
        println(s"$genre: $salesPercentage")
    }
  }


}
