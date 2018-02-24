# Kerberos Authentication

The goal of this document is describe:
1. What is Kerberos and how to setup it with Hadoop
2. how to setup and user Kerberos on a jvm application to execute commands over HDFS
3. how to execute commands on behalf of an user

## Introduction

Kerberos, developed at MIT, is designed to provide authentication for client-server application and for that it uses secret key-cryptography.

By default the Hadoop Cluster authentication is not secure since it is designed to trust all the credentials.
To overcome this vulnerability Kerberos is introduced in Hadoop Ecosystem which provides a secure way to verify the identity of users.

The core concepts are:

- principal
- Realm
- Key Distribution Center (KDC)

### Principal

A principal is an **identity** that needs to be verified. Kerberos support multiple types of identities.
 - User Principals Names: refer to users like the ones in an operating system
 - Service Principals Names: refers to services accessed by a user (e.g. a database)
 
### Realm

A realm refers to an authentication administrative domain. Principals are associated to realms in order
to demarcate boundaries and simplify administration.

> For example a **principal** can be user_daf while a realm can be **DAF.GOV.IT**.

### Key Distribution Center (KDC)

It contains all the information related to principals and realms. In particular,
it consists of 3 parts:
 
1. **A Database**: stores all principals and realms.
2. **An Authentication Service**: used to grant tickets to clients
3. **A Ticket Granting Service**: validate tickets 

## Kerberos with Hadoop

1. To start with, we first need to create a key distribution center (KDC) for the Hadoop cluster. 

> It is advisable to use a separate KDC for Hadoop which will be exclusive for Hadoop and should not be used by any other application.

2. To create service principals. We will create separate service principals for each of the Hadoop services i.e. mapreduce, yarn and hdfs.

3. To create Encrypted Kerberos Keys (Keytabs) for each service principal.

4. To install the Kerberos client in each node

4. The fourth step is to distribute keytabs for service principals to each of the cluster nodes.

5. The fifth and the last step is configuring all services to rely on kerberos authentication

The reference [2] explains the steps need to setup a kerberos with hadoop.

## Connect to a Kerberos Secured Hadoop Cluster on the JVM

Kerberos Authentication in Java is provided via the [Java Authentication and Authorization Service (JAAS)](https://en.wikipedia.org/wiki/Java_Authentication_and_Authorization_Service).
You can create credentials for an `hdfs-user` as follows on the KDC server:

```bash

(as root)
$ kadmin.local -q "addprinc -pw hadoop hdfs-user" 
$ kadmin.local -q "xst -k /home/hdfs-user/hdfs-user.keytab hdfs-user@MYCORP.NET"
(Creating a keytab will make the existing password invalid. To change your password back to hadoop use as root:)
$ kadmin.local -q "cpw -pw hadoop hdfs-user"

``` 

It creates a principal `hdfs-user` for the `MYCORP.NET` realm, and the file `/home/hdfs-user/hdfs-user.keytab`.

Additionally, we need to create a JAAS configuration

```bash

com.sun.security.jgss.krb5.initiate {
    com.sun.security.auth.module.Krb5LoginModule required
    doNotPrompt=true
    principal="hdfs-user@MYCORP.NET"
    useKeyTab=true
    keyTab="/home/hdfs-user/hdfs-user.keytab"
    storeKey=true;
};
```

We can list the different methods that can be used connecting HDFS with Kerberos.

> the one that I prefer is the `Via Keytab` since it does not require to execute a `kinit` via command line.

### Authentication with Keytab

It doesn't require code changes, but it requires additional parameters when the jar is run

1. you need to ensure that `conf.set("hadoop.security.authentication", "kerberos")` is set to kerberos
2. your jar is run with `-Djava.security.krb5.conf=/etc/krb5.conf \ -Djavax.security.auth.useSubjectCredsOnly=false \` as below

```bash

java -Djava.security.auth.login.config=/home/hdfs-user/jaas.conf \
-Djava.security.krb5.conf=/etc/krb5.conf \
-Djavax.security.auth.useSubjectCredsOnly=false \
-cp "./hdfs-sample-1.0-SNAPSHOT.jar:/usr/hdp/current/hadoop-client/lib/*:/usr/hdp/current/hadoop-hdfs-client/*:/usr/hdp/current/hadoop-client/*"  \
it.gov.daf.MainClass
 
```

### Via Keytab

This can be done injecting the `Principal@REALM` and the keytab file using the `UserGroupInformation`.

 ```java
 
Configuration conf = new Configuration();
conf.set("fs.defaultFS", "hdfs://one.hdp:8020");
conf.set("hadoop.security.authentication", "kerberos");
 
UserGroupInformation.setConfiguration(conf);
UserGroupInformation.loginUserFromKeytab("hdfs-user@MYCORP.NET", 
   "/home/hdfs-user/hdfs-user.keytab");
 
FileSystem fs = FileSystem.get(conf);
FileStatus[] fsStatus = fs.listStatus(new Path("/"));
for(int i = 0; i < fsStatus.length; i++){
  System.out.println(fsStatus[i].getPath().toString());
}
 
```

Please note here that we don't need to issue the password for the user.
[Connecting to HBase in a Kerberos Enabled Cluster](https://community.hortonworks.com/articles/48831/connecting-to-hbase-in-a-kerberos-enabled-cluster.html)
shows an additional example on how to connect to hbase.

### Using UserGroupInformation

For authentication in Hadoop there exists a wrapper class around a JAAS Subject to provide methods for user login. 
The UserGroupInformation wrapper without a specific setup uses the system security context, 
in case of Kerberos this exist in the ticket cache (klist shows the existing security context of a user). 
This is demonstrated under "With Existing Security Context" below. 
Further a custom security context can be used for login, either with by using a keytab file or even with credentials. 
Both approaches are also demonstrated here under "Providing Credentials from Login" and "Via Keytab".

1. First we need to authenticate

```bash

$ kinit 
Password for hdfs-user@MYCORP.NET: 
$ klist
Ticket cache: FILE:/tmp/krb5cc_1013
Default principal: hdfs-user@MYCORP.NET
 
Valid starting       Expires              Service principal
02/14/2016 14:54:32  02/15/2016 14:54:32  krbtgt/MYCORP.NET@MYCORP.NET

```

2. With this we can use the `UserGroupInformation` to execute secure commands

```java

Configuration conf = new Configuration();
conf.set("fs.defaultFS", "hdfs://one.hdp:8020");
conf.set("hadoop.security.authentication", "kerberos");
 
UserGroupInformation.setConfiguration(conf);
// Subject is taken from current user context
UserGroupInformation.loginUserFromSubject(null);
 
FileSystem fs = FileSystem.get(conf);
FileStatus[] fsStatus = fs.listStatus(new Path("/"));
 
for(int i = 0; i <= fsStatus.length; i++){
  System.out.println(fsStatus[i].getPath().toString());
}

```

### Providing Credentials

Providing login credentials at execution requires the creation of a javax.security.auth.Subject with username and password. This means that we will have to use the GSS-API to do a kinit like this:

```java

private static String username = "hdfs-user";
private static char[] password = "hadoop".toCharArray();
public static LoginContext kinit() throws LoginException {
  LoginContext lc = new LoginContext(HdfsMain.class.getSimpleName(), new CallbackHandler() {
  public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
    for(Callback c : callbacks){
      if(c instanceof NameCallback)
        ((NameCallback) c).setName(username);
      if(c instanceof PasswordCallback)
   
```

We still have to configure the JAAS login module referenced by the name that we provide in the above implementation. The name applied in the example above is set to be HdfsMain.class.getSimpleName(), so our module configuration should look like this:

```bash

HdfsMain {
  com.sun.security.auth.module.Krb5LoginModule required client=TRUE;
};

```

Having this in place we can now login with username and password:

```java

Configuration conf = new Configuration();
conf.set("fs.defaultFS", "hdfs://one.hdp:8020");
conf.set("hadoop.security.authentication", "kerberos");
UserGroupInformation.setConfiguration(conf);

//pay attention to the method call here
LoginContext lc = kinit();
UserGroupInformation.loginUserFromSubject(lc.getSubject());
 
FileSystem fs = FileSystem.get(conf);
FileStatus[] fsStatus = fs.listStatus(new Path("/"));
 
for(int i = 0; i < fsStatus.length; i++){
  System.out.println(fsStatus[i].getPath().toString());
}

```

## User Impersonation

The last aspect we analyze is how to use a `proxy-user` when executing commands.
The proxy user functionality enables services to access resources on the cluster on behalf of another user. 
Hence the service is impersonating the user.

It is important to control of who is able to impersonate whom and from where.
This can be configured by adding a proxy-user config to Hadoop

```
hadoop.proxyuser.{{service_user_name}}.groups
hadoop.proxuyser.{{service_user_name}}.hosts
```

For example is we have the `storage-service` running we can tell him to impersonate user of the host `web.mycorp.net".

```
hadoop.proxyuser.tomcat.groups=web-users
hadoop.proxyuser.tomcat.hosts=web.mycorp.net
```

> It is important to make sure service are not able to impersonate hdfs or other service account that have special privileges. 
> Only trusted services should be added to the proxyuser setup.

Clients can also use the UserGroupInformation class to impersonate other users. With the doAs implementation can be wrapped into the context of the user to be impersonated.

```java

// system/service user able to proxy
UserGroupInformation proxyUser = UserGroupInformation.getCurrentUser();
// user = user to impersonate
UserGroupInformation ugi = UserGroupInformation.createProxyUser(user, proxyUser);
try {
  fsStatus = ugi.doAs(new PrivilegedExceptionAction<FileStatus[]>() {
    public FileStatus[] run() throws IOException {
      return FileSystem.get(conf).listStatus(p);
    }
  });
} catch (InterruptedException e) { e.printStackTrace(); }
```

Here the user parameter defines the user (as String) in which context the call should be executed. The proxyUser is the current service or system user running the client. 
Be aware of the fact that not in all circumstances the proxyUser might be equal to `UserGroupInformation.getCurrentUser()`.

## Reference

1. [Install Kerberos on Redhat 7](https://gist.github.com/ashrithr/4767927948eca70845db)
2. [How to Setup Kerberos](https://examples.javacodegeeks.com/enterprise-java/apache-hadoop/hadoop-kerberos-authentication-tutorial/)
3. [Hadoop and Kerberos](https://www.gitbook.com/book/steveloughran/kerberos_and_hadoop/details)
4. [Connecting to HBase in a Kerberos Enabled Cluster](https://community.hortonworks.com/articles/48831/connecting-to-hbase-in-a-kerberos-enabled-cluster.html)