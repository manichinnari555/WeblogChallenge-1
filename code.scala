// PayTM Weblog Challenge 
// ------------------------ NOTES ------------------------------------------------
//
// Execute via Spark 1.6.0 spark-shell
// 
// The solution was done via Spark RDD APIs.I will pacakge the code using Maven once i get the more time  
// instead of just being a spark-shell script I would create a deployable application and associated unit tests.
//
// The solution assumes that 2015_07_22_mktplace_shop_web_log_sample.log.gz is stored in root HDFS folder
//
// The solution publishes to root HDFS folder a single part file for each of:
//  - unique_session_hits (911728 unique session hits)
//  - most_engaged_users_with_session (90544 users)
// I use coalesce to get 1 part. The output parts are included in output directory of project as part files and csv.
//
// The sessionization is performed via the default proposed 15 minute window. 
// 
// Optimal bin window analysis notes:
// An approach to identify the optimal window could look at the distribution 
// of intra-visit durations and choosing a thresolhold T such that X% of 
// clicks happen within T seconds. For simplicity, if we assume that for any T
// the number of sessions of a single click are neglible this is ok. To be more
// thorough it would be important to run through potential values for T,
// sessionize, and determine if the number of single click sessions  provides 
// appropriate X%.

// ------------- PART 1 : SESSIONIZE WITH ASSUMED 15 MINUTE WINDOW -----------------
//
// To sessionize the data, map visits to session creations 
// and roll them up to determine visit session number.
// 
// 1. load  data and parse out visits, sorted by ip and time
// 2. join visits on previous visit to tally sessions created on visit
// 3. determine session number for visit by rolling up session creation tally

// default 15 minute threshold value
val threshold = 900 // seconds

// assign data to RDD from HDFS zipped data file 
val data = sc.textFile("sample.log.gz")

// row to tuple parser - to parse string data rows to visitor tuples
// f: raw string visit -> (ip, time, rescode, reqcode, url_raw)
val time_format = new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSSS")
def parse(row: String) = {
		
	val pattern = """("[^"]+")|(\S+)""".r
	val original_fields = (pattern findAllIn row).toArray
	val request_fields = original_fields(11).replace("\"", "").split(" ")
	val user_agent = original_fields(12).replace("\"", "")
	val row_fields = original_fields.take(11) ++ 
		original_fields.takeRight(2) ++  
		request_fields :+ user_agent 

	val time = time_format.parse(row_fields(0)).getTime() / 1000
	val ip = row_fields(2).split(":")(0)
	val rescode = row_fields(8)
	val reqcode = row_fields(13)
	val url_raw = row_fields(14)

	(ip, time, rescode, reqcode, url_raw)
}

// visits = (ip, time, rescode, reqcode, url_raw) sorted by ip, time
val visits = data.map(r => parse(r)).sortBy(r => (r._1, r._2))

// (idx, visit)
val indx_visits = visits.zipWithIndex.map(r => (r._2, r._1))

// (idx + 1, (ip, time))
val indx_prev_visit_detail = indx_visits.map(r => (r._1 + 1, (r._2._1, r._2._2)))

// (idx, sess_created) sort by idx
val visit_session_tally = indx_visits.
  leftOuterJoin(indx_prev_visit_detail).
  map(x => (x._1, if (
		x._2._2.isEmpty ||                        // - no previous visit
		x._2._1._1 != x._2._2.get._1 ||           // - previous visit is not for ip 
		x._2._1._2 -  x._2._2.get._2 > threshold  // - exceed session threshold
	) 1 
	else 0)							          	  // - visit within threshold
).sortBy(x => x._1)

// Locally create cumulative sum of session creations. This is not scalable...
// I have a feeling there is probably a better way to do a running sum 
// but I was not able to find it in given time using pure RDDs.
val local_session_creations = visit_session_tally.collect()

val local_keys = local_session_creations.map(x => x._1)
val local_sesscreated = local_session_creations.map(x => x._2)
val local_sessions = local_sesscreated.scanLeft(0)(_ + _).drop(1)

val visit_sessions = sc.parallelize(local_keys.zip(local_sessions))

// (ip, time, rescode, reqcode, url_raw, session) sorted by ip, time
val sessionized_visits = indx_visits.join(visit_sessions).
	map(x => (x._2._1._1, x._2._1._2, x._2._1._3, x._2._1._4, x._2._1._5, x._2._2))

// ------------- PART 2 : FIND AVERAGE SESSION DURATION -----------------
// Take average session duration time to be total of all session durations
// divided by number of sessions in dataset. Assumes 1 hit in session is
// zero duration (ie. it decreases avg duration only)
//
// 1. downsample sessionized data to (session, time)
// 2. get session boundaries and calculate session duration
// 3. take ratio

//(session, time) -> (unique session, end time - start time)
val session_time = sessionized_visits.map(x => (x._6, x._2))
val session_start = session_time.reduceByKey((x, y) => if(x < y) x else y)
val session_end = session_time.reduceByKey((x, y) => if(x > y) x else y)
val session_duration = session_start.join(session_end).
	map(x => (x._1, x._2._2 - x._2._1))

val session_duration_total = session_duration.values.sum
val session_count = session_duration.count

// Double = 574.5947358163104
val average_session_duration = session_duration_total / session_count

// ------------- PART 3 : FIND UNIQUE URL HITS PER SESSION -----------------
// Make a list of (ip, session, url) and filter for uniques.
// I am not taking into account the HTTP method (GET/POST/PUT) not return codes
// I am not accounting for http URL differences not request params 
// so for example a page with paging params is counted 2x)

val unique_session_hits = sessionized_visits.map(x => (x._1, x._6, x._5)).distinct
unique_session_hits.coalesce(1).saveAsTextFile("unique_session_hits")

// ------------- PART 4 : FIND MOST ENGAGED USERS -----------------
// Get session duration time with ip. Take max session duration for ip. 
// Most engaged users are the descending sorted list of this result.

//((ip, session), time) -> (ip, session, duration, start, end)
val session_time = sessionized_visits.map(x => ((x._1, x._6), x._2))
val session_start = session_time.reduceByKey((x, y) => if(x < y) x else y)
val session_end = session_time.reduceByKey((x, y) => if(x > y) x else y)
val most_engaged_users_with_session = session_start.
	join(session_end)
	.map(x => (x._1._1, (x._1._1, x._1._2, x._2._2 - x._2._1, x._2._1, x._2._1))).
	reduceByKey((x, y) => if(x._3 > y._3) x else y).
	map(x => x._2).sortBy(x => -x._3)

most_engaged_users_with_session.coalesce(1).saveAsTextFile("most_engaged_users_with_session")
