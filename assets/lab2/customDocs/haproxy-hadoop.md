[![big-industries-logo](https://www.bigindustries.be/hubfs/popular-
bigindustries/big-industries-logo.svg)](//bigindustries.be)

  * [Home](https://www.bigindustries.be)
  * [Services](https://www.bigindustries.be/services)
  * [References](https://www.bigindustries.be/references)
  * [Team](https://www.bigindustries.be/our-team)
  * [Projects](https://www.bigindustries.be/projects)
  * [Jobs](https://www.bigindustries.be/jobs)
  * [Blogs](https://www.bigindustries.be/blog)

[ Contact ](https://www.bigindustries.be/contact)

######  [cloudera](https://www.bigindustries.be/blog/tag/cloudera)

# Wrapping multiple backend Hadoop web applications with HAProxy

![Robert Gibbon](https://www.bigindustries.be/hs-
fs/hubfs/Imported_Blog_Media/Robert-Big-Industries-Big-Data-Consulting-
Belgium-SI-Systems-Integration-Data-Science-Applications-R-Hadoop-Impala-
Scala-Spark-MapReduce-MapR-Cloudera.jpg?width=56&name=Robert-Big-Industries-
Big-Data-Consulting-Belgium-SI-Systems-Integration-Data-Science-Applications-
R-Hadoop-Impala-Scala-Spark-MapReduce-MapR-Cloudera.jpg)

[ Robert Gibbon ](https://www.bigindustries.be/blog/author/robert-gibbon)

Dec 26, 2017 3:41:27 PM

SHARE [
](https://www.facebook.com/sharer/sharer.php?u=https%3A%2F%2Fwww.bigindustries.be%2Fblog%2Fwrapping-
multiple-backend-hadoop-web-applications-with-haproxy) [
](https://twitter.com/intent/tweet?original_referer=https%3A%2F%2Fwww.bigindustries.be%2Fblog%2Fwrapping-
multiple-backend-hadoop-web-applications-with-
haproxy&url=https%3A%2F%2Fwww.bigindustries.be%2Fblog%2Fwrapping-multiple-
backend-hadoop-web-applications-with-
haproxy&source=tweetbutton&text=Wrapping+multiple+backend+Hadoop+web+applications+with+HAProxy)
[
](https://www.linkedin.com/shareArticle?mini=true&url=https%3A%2F%2Fwww.bigindustries.be%2Fblog%2Fwrapping-
multiple-backend-hadoop-web-applications-with-
haproxy&title=Wrapping+multiple+backend+Hadoop+web+applications+with+HAProxy&summary=Authorizing+access+to+multiple+Hadoop+...)
[
](mailto:?subject=Check%20out%20Wrapping%20multiple%20backend%20Hadoop%20web%20applications%20with%20HAProxy%20&body=Check%20out%20https://www.bigindustries.be/blog/wrapping-
multiple-backend-hadoop-web-applications-with-haproxy)

Authorizing access to multiple Hadoop applications on different nodes of the
cluster can be complex and troublesome for some organizations.

In order to assure a consistent access access path, ideally we want to expose
all web applications via a single entry point. In this example, we will use
HAProxy to aggregate a bunch of Hadoop backend web applications and expose
them from a single host and port.

#### HAProxy

HAProxy is a free, open source software load balancer with some nice features,
including some features specifically for HTTP traffic.

http://www.haproxy.org/

Most Linux distributions include a version of HAProxy, and while it might not
be the latest and greatest, the default version that comes with your Linux
distro is probably going to be sufficient for what we want to do in this
example.

#### Setting Up

We will simulate the Hue, Oozie and Cloudera Manager backend web apps using
the python SimpleHTTPserver module. The python SimpleHTTPserver module enables
us to serve up a directory listing of the present working directory of the
job.

We will create some placeholder content and serve it up on the ports used by
the real applications.

cd ~

mkdir -p hue

echo “hello hue” > hue/hellohue.txt



mkdir -p oozie

echo “hello oozie” > oozie/hellooozie.txt



mkdir -p cm

echo “hello Cloudera Manager” > cm/hellocm.txt



cd hue

python -m SimpleHTTPServer 8888 &

cd ..



cd oozie

python -m SimpleHTTPServer 11000 &

cd ..



cd cm

python -m SimpleHTTPServer 7180 &

cd ..

In order to route to the right backend, we need to have a way to tell HAProxy
which backend to route to. One way is to use alternative host names and have
HAProxy inspect the hostname, however this may be unacceptably complex for
some organizations, so instead we will rely on the root path.

So if, for example, the user enters the path
http://loadbalancer.fqdn.org:8080/cm then we want HAProxy to route this
request and all subsequent requests to Cloudera Manager.

If, on the other hand, the user enters the path
http://loadbalancer.fqdn.org:8080/hue then we want HAProxy to route this
request and all subsequent requests to Hue.

Finally if the user enters the path http://loadbalancer.fqdn.org:8080/oozie
then we want HAProxy to route this request and all subsequent requests to
Oozie.

Seems simple right? Well, no, because the backend applications are not
listening at /oozie and /cm and /hue. They are all listening a the root of the
given backend webserver, /.

Furthermore, subrequests, for example for javascript, css, and images, might
be on other paths beneath /. How will the loadbalancer know to send them to
the right backend?

Lastly, when the user follows a link in the application, how will the load
balancer know which backend to send the request to?

The answer to these questions is to set a cookie. When the first request comes
in, we strip the application identifier from the path and then send the
request to the appropriate backend, setting a cookie that identifies the
current application at the same time.

When the next request comes in, the application identifier won’t be on the
path, but we know which backend to send the request to - based on the cookie.

When the user wants to access another application, he just has to enter the
application path for the other application, and HAProxy will know to first
strip the application identifier from the path, then set a cookie, and forward
this and subsequent requests to the other application backend.

Here’s a simple example of how the HAProxy configuration file would look:

 defaults

    log     global

    mode    http

    timeout connect 5000

    timeout client  50000

    timeout server  50000



frontend webfe

    bind *:8080

    mode http



    acl is_hue_path path_beg -i /hue

    acl is_cm_path path_beg -i /cm

    acl is_oozie_path path_beg -i /oozie



    acl is_hue_cookie hdr_sub(cookie) BACKEND=hue

    acl is_cm_cookie hdr_sub(cookie) BACKEND=cm

    acl is_oozie_cookie hdr_sub(cookie) BACKEND=oozie



    use_backend hue if is_hue_path

    use_backend cm if is_cm_path

    use_backend oozie if is_oozie_path

    use_backend hue if is_hue_cookie

    use_backend cm if is_cm_cookie

    use_backend oozie if is_oozie_cookie



backend hue

    mode http

    balance roundrobin

    option forwardfor



    http-request set-header X-Forwarded-Port %[dst_port]

    http-request add-header X-Forwarded-Proto https if { ssl_fc }

    cookie BACKEND insert indirect nocache



    reqirep ^([^\ :]*)\ /hue([^\ ]*)\ (.*)$       \1\ /\2\ \3

    rspirep ^(Location:)\ http://([^/]*)/(.*)$    \1\ http://\2/hue/\3

    rspirep ^(Set-Cookie:.*\ path=)([^\ ]+)(.*)$       \1/hue\2\3



    server hue01 localhost:8888 cookie hue   



backend cm

    mode http

    balance roundrobin

    option forwardfor



    http-request set-header X-Forwarded-Port %[dst_port]

    http-request add-header X-Forwarded-Proto https if { ssl_fc }

    cookie BACKEND insert indirect nocache



    reqirep ^([^\ :]*)\ /cm([^\ ]*)\ (.*)$       \1\ /\2\ \3

    rspirep ^(Location:)\ http://([^/]*)/(.*)$    \1\ http://\2/cm/\3

    rspirep ^(Set-Cookie:.*\ path=)([^\ ]+)(.*)$       \1/cm\2\3



    server cm01 localhost:7180 cookie cm



backend oozie

    mode http

    balance roundrobin

    option forwardfor



    http-request set-header X-Forwarded-Port %[dst_port]

    http-request add-header X-Forwarded-Proto https if { ssl_fc }

    cookie BACKEND insert indirect nocache



    reqirep ^([^\ :]*)\ /oozie([^\ ]*)\ (.*)$       \1\ /\2\ \3

    rspirep ^(Location:)\ http://([^/]*)/(.*)$    \1\ http://\2/oozie/\3

    rspirep ^(Set-Cookie:.*\ path=)([^\ ]+)(.*)$       \1/oozie\2\3



    server oozie01 localhost:11000 cookie oozie



To test, fire up haproxy in foreground with the config file:

haproxy -f our_test_haproxy.cfg



And try to browse to the HAProxy paths:

![Directory.png](https://www.bigindustries.be/hs-
fs/hubfs/Directory.png?width=700&height=489&name=Directory.png)



Cool, looks like that works. Will browsing to the file work?



![Hello Hue.png](https://www.bigindustries.be/hs-
fs/hubfs/Hello%20Hue.png?width=700&height=487&name=Hello%20Hue.png)



What about now browsing to our Cloudera Manager url?



![Directory listing.png](https://www.bigindustries.be/hs-
fs/hubfs/Directory%20listing.png?width=700&height=491&name=Directory%20listing.png)



looks good



![Hello Cloudera Manager.png](https://www.bigindustries.be/hs-
fs/hubfs/Hello%20Cloudera%20Manager.png?width=700&height=487&name=Hello%20Cloudera%20Manager.png)



Now onto Oozie



![Hello Oozie.png](https://www.bigindustries.be/hs-
fs/hubfs/Hello%20Oozie.png?width=700&height=488&name=Hello%20Oozie.png)



Yes!!, seems to work



![Seems to work.png](https://www.bigindustries.be/hs-
fs/hubfs/Seems%20to%20work.png?width=700&height=487&name=Seems%20to%20work.png)



 [![Contact us if you need help with your Cloudera project](https://no-
cache.hubspot.com/cta/default/2240994/a86d3eef-963f-44fb-86c4-c99beee76544.png)](https://cta-
redirect.hubspot.com/cta/redirect/2240994/a86d3eef-963f-44fb-86c4-c99beee76544)



















![Robert Gibbon](https://www.bigindustries.be/hs-
fs/hubfs/Imported_Blog_Media/Robert-Big-Industries-Big-Data-Consulting-
Belgium-SI-Systems-Integration-Data-Science-Applications-R-Hadoop-Impala-
Scala-Spark-MapReduce-MapR-Cloudera.jpg?width=66&name=Robert-Big-Industries-
Big-Data-Consulting-Belgium-SI-Systems-Integration-Data-Science-Applications-
R-Hadoop-Impala-Scala-Spark-MapReduce-MapR-Cloudera.jpg)

#### [ Robert Gibbon ](https://www.bigindustries.be/blog/author/robert-gibbon)

Rob is a Hadoop and large-scale distributed computing evangelist. Solution
Architect by trade, Rob is a managing partner at Big Industries - the premiere
Hadoop & Big Data systems integrator for Belgium and Luxembourg.

[ ](https://be.linkedin.com/in/robertgibbon)

## Related posts

At Big Industries, we want to share our wisdom. More info on our recent
developments, newest projects and upcoming events

[ ![Top Customer Kafka & Streaming data requests and how Big Industries can
help](https://www.bigindustries.be/hubfs/top%20customer%20request%20streaming-1.png)
](https://www.bigindustries.be/blog/top-customer-kafka-streaming-data-
requests-and-how-big-industries-can-help)

######  [Confluent](https://www.bigindustries.be/blog/tag/confluent)

#### [Top Customer Kafka & Streaming data requests and how Big Industries can
help](https://www.bigindustries.be/blog/top-customer-kafka-streaming-data-
requests-and-how-big-industries-can-help)

Data streams can be processed on a record-by-record basis or over sliding time
windows, and used...

[ Read more ](https://www.bigindustries.be/blog/top-customer-kafka-streaming-
data-requests-and-how-big-industries-can-help)

[ ![Tableau](//www.bigindustries.be/wp-content/uploads/2015/12/Tableau.png)
](https://www.bigindustries.be/blog/fast-business-intelligence-for-all-with-
hadoop-and-tableau)

######  [cloudera](https://www.bigindustries.be/blog/tag/cloudera)

#### [Fast Business Intelligence For All with Hadoop and
Tableau](https://www.bigindustries.be/blog/fast-business-intelligence-for-all-
with-hadoop-and-tableau)

Hadoop has forever changed the way we deal with data. Its ability to support
parallel processing...

[ Read more ](https://www.bigindustries.be/blog/fast-business-intelligence-
for-all-with-hadoop-and-tableau)

#### Subscribe to our newsletter

Stay informed about our recent developments, newest projects and upcoming
events

###

![img-
mails](https://f.hubspotusercontent10.net/hubfs/369261/raw_assets/public/Marketplace/leadstreet/themes/popular-
theme/images/img-mails.svg)

## Ready to set off on a BIG journey?

##### The top notch technologies we use set us apart from other consultancies  
  

[HAVE A LOOK AT OUR SERVICES](https://www.bigindustries.be/services)

**Big Industries NV**

**YOU THINK BIG, YOU GET BIG**

##  

[ ](https://www.facebook.com/Big-Industries-933906943382629/) [
](https://www.linkedin.com/company/4838922/)

**Contact us**

Veldkant 33a,  
2550 Kontich, Belgium  
[GET DIRECTIONS ](//www.bigindustries.be/contact)  
  
T +32 (0)3 450 80 30  
E [info@bigindustries.be](mailto:info@bigindustries.be)

**Hours**

Monday to Friday  
9am – 5pm CET  
  
Weekends  
Closed

##### **Drop us a line**

© Big Industries 2021 - all rights reserved

  * [TERMS AND CONDITIONS](https://www.bigindustries.be/terms-and-conditions)
  * [PRIVACY POLICY](https://www.bigindustries.be/privacy-policy)

