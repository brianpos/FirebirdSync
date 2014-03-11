FirebirdSync
=

The FirebirdSync project is a .NET demonstration application to show how to create 
a Firebird Provider for Microsoft Sync Services 2.1

It is not intended as a complete howto, and is only provided as-is without doing much
testing. That being said, it is a lot more complete than any other code I could find
easily out there.

The basic part of this is the implementation of a DbSyncProvider derived class for Firebird.

The word document **Firebird Sync Services Demo.docx** contains the pre-requisites, 
installation and configuration notes, and also a complete rundown of a demonstration run.

Before you start anything here, have a look in this document and review the annotated demonstration
script with screenshots to see if it is what you are looking for.

Credits
-
This was a direct port on the example provided by Microsoft to implement a provider
for Oracle.

[Microsoft Download Database Sync for Oracle](http://code.msdn.microsoft.com/Database-Sync-Oracle-and-037fb083/sourcecode?fileId=19015&pathId=1409552837)


Notes
-
There were 2 issues that plagued me while building this:

1. The firebird Data Provider Command Objects don't care about the naming of the 
parameters, only the order that they appear in the Parameters object.
So you need to be very aware of the order in the Firebird SQL Stored Procedures and
the code.

2. The Microsoft Sync Services internals calls the GetScope routine repeatedly and 
have a Dispose on the command object **SelectScopeInfoCommand** which means that
wile processing later on you get commands being executed that have no text.
The workaround I did was to have a **RecreateSelectScope** routine that is called
before the base class in 4 overloaded virtual functions.
