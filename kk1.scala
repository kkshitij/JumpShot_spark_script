
/*
author : Kshitij

Description - The script reads :
1) the input file having time,userid,url
2) tuple with intereting clicks is in the code -> 	("acme.com","/thank-you"),("cooldeals.com","/acme-offer"),("domain2.com","/path3")	  
3) output is in format userid,time,url,Referred url

*/

val inp = sc.textFile("/user/kshitijkumar068300/jumpshot_part-00000").map(_.split('\t')).map(x => (x(1)) -> (x(0).toInt , x(2)))

// Read the input file with userId as key

val o = inp.aggregateByKey(List[(Int,String)]())(                                              //create a RDD having userId as the key and all its clicks,timestamp as list
  (aggr, value) => value :: aggr,
  (aggr1, aggr2) => aggr1 ::: aggr2
)

val intrstTup = sc.parallelize(List(("acme.com","/thank-you"),("cooldeals.com","/acme-offer"),("domain2.com","/path3")))    //interesting clicks 

val bc = sc.broadcast(intrstTup.map((x) => (x._1 + x._2) -> ("")).collectAsMap)                // if the size is small collect & broadcast it 

val bcMap = bc.value 

val kk = o.mapPartitions( it => {
 var list2 = List[(String,Int,String,String)]()

  while(it.hasNext) 
 {
 val a = it.next
 
 val list1 = a._2.sortBy(_._1)                                                                   // imp to sort on time for finding back the referral click 
 
 list1.foreach                                                                                   //for every user id ..
   { x => 
    {
 var ur = new java.net.URL(x._2)                                   
 if(bcMap.contains((ur.getHost + ur.getPath).toString))                                           //lookup to the interesting clicks broadcasted variable
    { 
       var done : Boolean = true
       
      for(i <- list1.indexOf(x)-1 to 0 by -1 ; if done == true)                                    //Finding referral click
      {
           if(!(new java.net.URL(list1(i)._2).getHost.equals(new java.net.URL(x._2).getHost)))  
            {
               var s = list1(i)._2
               list2 = list2 :+ (a._1,x._1,x._2,s)
               done = false                                                                          //a function with return can be used here 
         
            }
           else {done = true}
      }
    } 
 
    }
  }   
 }
   list2.iterator                
   }
 )

kk.collect.foreach(println)