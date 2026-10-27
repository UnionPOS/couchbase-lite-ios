//
//  ReplicatorInternal_Tests.m
//  CouchbaseLite
//
//  Created by Jens Alfke on 12/23/14.
//
//

#import "CBLTestCase.h"
#import "CouchbaseLitePrivate.h"
#import "CBLRestReplicator+Internal.h"
#import "CBLRestPuller.h"
#import "CBLRestPusher.h"
#import "CBLReachability.h"
#import "CBL_Server.h"
#import "CBLDatabase+Replication.h"
#import "CBLDatabase+Insertion.h"
#import "CBLRevision.h"
#import "CBLOAuth1Authorizer.h"
#import "CBLBase64.h"
#import "CBLInternal.h"
#import "CBLMisc.h"
#import "BLIPHTTPLogic.h"
#import "MYURLUtils.h"
#import "CBLHTTPListener.h"


// This db will get deleted and overwritten during every test.
#define kScratchDBName @"cbl_replicator_scratch"

// This is a read-only db whose contents should be a default CouchApp.
#define kCouchAppDBName @"couchapp_helloworld"


#define replic8(DB, REMOTE, PUSH, FILTER, DOCIDS, ERR) \
    [self replicate: REMOTE push: PUSH filter: FILTER docIDs: DOCIDS expectError: ERR]

#define replic8Continuous(DB, REMOTE, PUSH, FILTER, OPTIONS, ERR) \
    [self replicateContinuous: REMOTE push: PUSH filter: FILTER options: OPTIONS expectError: ERR]


@interface CBLManager (Seekrit)
- (CBLStatus) parseReplicatorProperties: (NSDictionary*)properties
                            toDatabase: (CBLDatabase**)outDatabase   // may be NULL
                                remote: (NSURL**)outRemote          // may be NULL
                                isPush: (BOOL*)outIsPush
                          createTarget: (BOOL*)outCreateTarget
                               headers: (NSDictionary**)outHeaders
                            authorizer: (id<CBLAuthorizer>*)outAuthorizer;
@end


@interface ReplicatorInternal_Tests : CBLTestCaseWithDB
@end


@implementation ReplicatorInternal_Tests
{
    BOOL _newReplicator;
}


- (void)invokeTest {
    // Run each test method twice, once with the old replicator and once with the new.
    _newReplicator = NO;
    [super invokeTest];
    if ([[NSUserDefaults standardUserDefaults] boolForKey: @"TestNewReplicator"]) {
        _newReplicator = YES;
        [super invokeTest];
    }
}

- (void) setUp {
    if (_newReplicator)
        Log(@"++++ Now using new replicator");
    [super setUp];
    if (_newReplicator) {
        dbmgr.replicatorClassName = @"CBLBlipReplicator";
        dbmgr.dispatchQueue = dispatch_get_main_queue();
    }
}


- (void) test_00_Pusher_From_Empty {
    NSURL* remoteURL = [self remoteTestDBURL: kScratchDBName];
    if (!remoteURL)
        return;
    [self eraseRemoteDB: remoteURL];

    id lastSeq = replic8(db, remoteURL, YES, nil, nil, nil);
    AssertEqual(lastSeq, nil);
}


- (void) test_01_Pusher {
    RequireTestCase(CBLDatabase);
    __block int filterCalls = 0;
    __weak ReplicatorInternal_Tests* weakSelf = self;
    [db setFilterNamed: @"filter" asBlock: ^BOOL(CBLSavedRevision *revision, NSDictionary* params) {
        ReplicatorInternal_Tests* self = weakSelf;
        //Log(@"Test filter called with params = %@", params);
        //Log(@"Rev = %@, properties = %@", revision, revision.properties);
        Assert(revision.properties);
        ++filterCalls;
        return YES;
    }];

    // Create some documents:
    NSMutableDictionary* props = $mdict({@"_id", @"doc1"},
                                        {@"foo", @1}, {@"bar", $false});
    CBLStatus status;
    NSError* error;
    CBL_Revision* rev1 = [db putRevision: [CBL_MutableRevision revisionWithProperties: props]
                          prevRevisionID: nil allowConflict: NO status: &status error: &error];
    AssertEq(status, kCBLStatusCreated);
    AssertNil(error);
    
    props.cbl_rev = rev1.revID;
    props[@"UPDATED"] = $true;
    CBL_Revision* rev2 = [db putRevision: [CBL_MutableRevision revisionWithProperties: props]
                          prevRevisionID: rev1.revID allowConflict: NO
                                  status: &status error: &error];
    AssertEq(status, kCBLStatusCreated);
    AssertNil(error);
    
    props = $mdict({@"_id", @"doc2"},
                   {@"baz", @(666)}, {@"fnord", $true});
    [db putRevision: [CBL_MutableRevision revisionWithProperties: props ]
     prevRevisionID: nil allowConflict: NO
             status: &status error: &error];
    AssertEq(status, kCBLStatusCreated);
    AssertNil(error);
#pragma unused(rev2)

    [self createDocuments: 100];
    
    // Push them to the remote:
    NSURL* remoteDB = [self remoteTestDBURL: kScratchDBName];
    if (!remoteDB)
        return;
    [self eraseRemoteDB: remoteDB];
    id lastSeq = replic8(db, remoteDB, YES, @"filter", nil, nil);
    AssertEq([lastSeq intValue], 103);
    AssertEq(filterCalls, 102);
}


- (void) preparePullReplicationTest {
    // Create some documents:
    NSMutableDictionary* props = $mdict({@"_id", @"doc1"},
                                        {@"foo", @1}, {@"bar", $false});
    CBLStatus status;
    NSError* error;
    CBL_Revision* rev1 = [db putRevision: [CBL_MutableRevision revisionWithProperties: props]
                          prevRevisionID: nil allowConflict: NO status: &status error: &error];
    AssertEq(status, kCBLStatusCreated);
    AssertNil(error);
    
    props.cbl_rev = rev1.revID;
    props[@"UPDATED"] = $true;
    CBL_Revision* rev2 = [db putRevision: [CBL_MutableRevision revisionWithProperties: props]
                          prevRevisionID: rev1.revID allowConflict: NO
                                  status: &status error: &error];
    Assert(rev2 != nil);
    AssertEq(status, kCBLStatusCreated);
    AssertNil(error);
    
    props = $mdict({@"_id", @"doc2"},
                   {@"baz", @(666)}, {@"fnord", $true});
    [db putRevision: [CBL_MutableRevision revisionWithProperties: props ]
     prevRevisionID: nil allowConflict: NO
             status: &status error: &error];
    AssertEq(status, kCBLStatusCreated);
    AssertNil(error);
    
    [self createDocuments: 100];
    
    // Push them to the remote:
    NSURL* remoteURL = [self remoteTestDBURL: kScratchDBName];
    if (!remoteURL)
        return;
    
    [self eraseRemoteDB: remoteURL];
    id lastSeq = replic8(db, remoteURL, YES, nil, nil, nil);
    AssertEq([lastSeq intValue], 103);
    
    // Erase test db:
    [self eraseTestDB];
}


- (void) test_02_Puller {
    [self preparePullReplicationTest];
    
    NSURL* remoteURL = [self remoteTestDBURL: kScratchDBName];
    if (!remoteURL)
        return;
    
    // Pull from remote:
    id lastSeq = replic8(db, remoteURL, NO, nil, nil, nil);
    AssertEqual(lastSeq, @103);
    
    AssertEq(db.documentCount, 102u);
    AssertEq(db.lastSequenceNumber, (self.isSQLiteDB ? 103 : 102));
    
    // Replicate again; should complete but add no revisions:
    Log(@"Second replication, should get no more revs:");
    replic8(db, ([self remoteTestDBURL: kScratchDBName]), NO, nil, nil, nil);
    AssertEq(db.lastSequenceNumber, (self.isSQLiteDB ? 103 : 102));
    
    CBL_Revision* doc = [db getDocumentWithID: @"doc1" revisionID: nil];
    Assert(doc);
    Assert([doc.revIDString hasPrefix: @"2-"]);
    AssertEqual(doc[@"foo"], @1);
    
    doc = [db getDocumentWithID: @"doc2" revisionID: nil];
    Assert(doc);
    Assert([doc.revIDString hasPrefix: @"1-"]);
    AssertEqual(doc[@"fnord"], $true);
}


- (void) test_03_Puller_Continuous {
    [self preparePullReplicationTest];
    
    NSURL* remoteURL = [self remoteTestDBURL: kScratchDBName];
    if (!remoteURL)
        return;
    
    // Pull from remote:
    id lastSeq = replic8Continuous(db, remoteURL, NO, nil, nil, nil);
    AssertEqual(lastSeq, @103);

    AssertEq(db.documentCount, 102u);
    AssertEq(db.lastSequenceNumber, (self.isSQLiteDB ? 103 : 102));

    // Replicate again; should complete but add no revisions:
    Log(@"Second replication, should get no more revs:");
    replic8Continuous(db, remoteURL, NO, nil, nil, nil);
    AssertEq(db.lastSequenceNumber, (self.isSQLiteDB ? 103 : 102));

    CBL_Revision* doc = [db getDocumentWithID: @"doc1" revisionID: nil];
    Assert(doc);
    Assert([doc.revIDString hasPrefix: @"2-"]);
    AssertEqual(doc[@"foo"], @1);

    doc = [db getDocumentWithID: @"doc2" revisionID: nil];
    Assert(doc);
    Assert([doc.revIDString hasPrefix: @"1-"]);
    AssertEqual(doc[@"fnord"], $true);
}


- (void) test_04_Puller_Continuous_PermanentError {
    RequireTestCase(Puller);
    NSURL* remoteURL = [self remoteTestDBURL: @"non_existent_remote_db"];
    if (!remoteURL)
        return;

    NSError* error = CBLStatusToNSError(kCBLStatusNotFound);
    replic8Continuous(db, remoteURL, NO, nil, nil, error);
}


- (void) test_04_Pusher_Continuous_PermanentError {
    RequireTestCase(Puller);
    NSURL* remoteURL = [self remoteTestDBURL: @"non_existent_remote_db"];
    if (!remoteURL)
        return;

    [self createDocuments: 1];

    NSError* error = CBLStatusToNSError(kCBLStatusNotFound);
    NSError* gotError = replic8Continuous(db, remoteURL, YES, nil, nil, error);
    // Verify that CBLRemoteRequest picked up the error message from the server's JSON response:
    AssertEqual(gotError.localizedDescription, @"no such database \"non_existent_remote_db\"");
}


- (void) test_05_Puller_DatabaseValidation {
    [self preparePullReplicationTest];

    NSURL* remote = [self remoteTestDBURL: kScratchDBName];
    if (!remote)
        return;

    [db setValidationNamed:@"OnlyDoc1" asBlock:^(CBLRevision *newRevision, id<CBLValidationContext> context) {
        if (![newRevision.document.documentID isEqualToString:@"doc1"]) {
            [context reject];
        }
    }];

    // Start a named document pull replication.
    CBL_ReplicatorSettings* settings = [[CBL_ReplicatorSettings alloc] initWithRemote: remote push: NO];
    settings.authorizer = self.authorizer;
    id<CBL_Replicator> repl = [[dbmgr.replicatorClass alloc] initWithDB: db settings: settings];
    [repl start];

    // Let the replicator run.
    Assert(repl.status != kCBLReplicatorStopped);
    Log(@"Waiting for replicator to finish...");
    while (repl.status != kCBLReplicatorStopped || repl.savingCheckpoint) {
        if (![[NSRunLoop currentRunLoop] runMode: NSDefaultRunLoopMode
                                      beforeDate: [NSDate dateWithTimeIntervalSinceNow: 0.5]])
            break;
    }
    AssertEq(repl.status, kCBLReplicatorStopped);
    Assert(!repl.savingCheckpoint);
    AssertNil(repl.error);
    Log(@"...replicator finished. lastSequence=%@", repl.lastSequence);
    id lastSeq = repl.lastSequence;
    AssertEqual(lastSeq, @103);

    AssertEq(db.documentCount, 1u);
    AssertEq(db.lastSequenceNumber, (self.isSQLiteDB ? 2 : 1));

    CBL_Revision* doc = [db getDocumentWithID: @"doc1" revisionID: nil];
    Assert(doc);
    Assert([doc.revIDString hasPrefix: @"2-"]);
    AssertEqual(doc[@"foo"], @1);
}


- (void) test_06_Puller_Authenticate {
    RequireTestCase(Puller);
    NSURL* remoteURL = [self remoteTestDBURL: @"cbl_auth_test"];
    if (!remoteURL)
        return;

    CBL_ReplicatorSettings* settings = [[CBL_ReplicatorSettings alloc] initWithRemote: remoteURL
                                                                                 push: NO];
    [self replicate: settings expectError: CBLStatusToNSError(kCBLStatusUnauthorized)];

    settings.authorizer = [[CBLPasswordAuthorizer alloc] initWithUser: @"test" password: @"abc123"];
    [self replicate: settings expectError: nil];
}


- (void) test_06_Puller_SSL {
    if (_newReplicator) {
        Warn(@"Skipping SSL tests until new replicator supports cert validation");
        return;
    }
    RequireTestCase(Pusher);
    NSURL* remoteURL = [self remoteSSLTestDBURL: @"public"];
    if (!remoteURL)
        return;

    Log(@"Replicating without root cert; should fail...");
    CBLSetAnchorCerts(nil, NO);
    [self allowWarningsIn:^{
        replic8(db, remoteURL, NO, nil, nil,
                ([NSError errorWithDomain: NSURLErrorDomain
                                    code: NSURLErrorServerCertificateUntrusted userInfo: nil]));
    }];

    Log(@"Now replicating with root cert installed...");
    CBLSetAnchorCerts([self remoteTestDBAnchorCerts], NO);
    id lastSeq = replic8(db, remoteURL, NO, nil, nil, nil);
    CBLSetAnchorCerts(nil, NO);
    Assert([lastSeq intValue] >= 2);

    AssertEq(db.documentCount, 2u);
    AssertEq(db.lastSequenceNumber, 2);
}

- (void) test_07_Puller_SSL_Continuous {
    if (_newReplicator) {
        Warn(@"Skipping SSL tests until new replicator supports cert validation");
        return;
    }
    RequireTestCase(Pusher);
    NSURL* remoteURL = [self remoteSSLTestDBURL: @"public"];
    if (!remoteURL)
        return;

    Log(@"Replicating without root cert; should fail...");
    CBLSetAnchorCerts(nil, NO);
    [self allowWarningsIn:^{
        replic8Continuous(db, remoteURL, NO, nil, nil,
                          [NSError errorWithDomain: NSURLErrorDomain
                                              code: NSURLErrorServerCertificateUntrusted
                                          userInfo: nil]);
    }];

    Log(@"Now replicating with root cert installed...");
    CBLSetAnchorCerts([self remoteTestDBAnchorCerts], NO);
    id lastSeq = replic8Continuous(db, remoteURL, NO, nil, nil, nil);
    CBLSetAnchorCerts(nil, NO);
    Assert([lastSeq intValue] >= 2);

    AssertEq(db.documentCount, 2u);
    AssertEq(db.lastSequenceNumber, 2);
}

- (void) test_08_Puller_SSL_Pinned {
    if (_newReplicator) {
        Warn(@"Skipping SSL tests until new replicator supports cert validation");
        return;
    }
    RequireTestCase(Puller_SSL_Continuous);
    NSURL* remoteURL = [self remoteSSLTestDBURL: @"public"];
    if (!remoteURL)
        return;

    Log(@"Replicating with wrong pinned cert; should fail...");
    [self allowWarningsIn:^{
        NSString* digest = @"123456789abcdef0123456789abcdef012345678";
        replic8Continuous(db, remoteURL, NO, nil, @{kCBLReplicatorOption_PinnedCert: digest},
                          [NSError errorWithDomain: NSURLErrorDomain
                                              code: NSURLErrorServerCertificateUntrusted
                                          userInfo: nil]);
    }];

    Log(@"Now replicating with correct pinned cert...");
    NSString* digest = CBLHexSHA1Digest([self contentsOfTestFile: @"SelfSigned.cer"]);
    id lastSeq = replic8Continuous(db, remoteURL, NO, nil,
                                   @{kCBLReplicatorOption_PinnedCert: digest},
                                   nil);
    CBLSetAnchorCerts(nil, NO);
    Assert([lastSeq intValue] >= 2);

    AssertEq(db.documentCount, 2u);
    AssertEq(db.lastSequenceNumber, 2);
}

- (void) test_09_Pusher_NonExistentServer {
    RequireTestCase(Pusher);
    [self createDocuments: 1];
    NSURL* remoteURL = [NSURL URLWithString:@"https://mylocalhost/db"];
    if (!remoteURL) {
        Warn(@"Skipping test CBL_Pusher_NonExistentServer: invalid URL");
        return;
    }

    replic8(db, remoteURL, YES, nil, nil, [NSError errorWithDomain: NSURLErrorDomain
                                                              code: NSURLErrorCannotFindHost
                                                          userInfo: nil]);
}

- (void) test_10_Puller_NonExistentServer {
    RequireTestCase(Puller);
    NSURL* remoteURL = [NSURL URLWithString:@"https://mylocalhost/db"];
    if (!remoteURL) {
        Warn(@"Skipping test CBL_Puller_NonExistentServer: invalid URL");
        return;
    }

    replic8(db, remoteURL, NO, nil, nil, [NSError errorWithDomain: NSURLErrorDomain
                                                             code: NSURLErrorCannotFindHost
                                                         userInfo: nil]);
}


#if 0 // FIX: Sync Gateway doesn't support this yet!
- (void) test_11_Puller_DocIDs {
    RequireTestCase(Pusher); // CBL_Pusher populates the remote db that this test pulls from...
    
    NSURL* remote = [self remoteTestDBURL: kScratchDBName];
    if (!remote) {
        Warn(@"Skipping test: no remote URL");
        return;
    }

    // Start a named document pull replication.
    CBL_Replicator* repl = [[CBL_Replicator alloc] initWithDB: db remote: remote
                                                     push: NO continuous: NO];
    repl.docIDs = @[@"doc1"];
    repl.authorizer = self.authorizer;
    [repl start];
    
    // Let the replicator run.
    Assert(repl.status != kCBLReplicatorStopped);
    Log(@"Waiting for replicator to finish...");
    while (repl.status != kCBLReplicatorStopped || repl.savingCheckpoint) {
        if (![[NSRunLoop currentRunLoop] runMode: NSDefaultRunLoopMode
                                      beforeDate: [NSDate dateWithTimeIntervalSinceNow: 0.5]])
            break;
    }
    AssertEq(repl.status, kCBLReplicatorStopped);
    Assert(!repl.savingCheckpoint);
    AssertNil(repl.error);
    Log(@"...replicator finished. lastSequence=%@", repl.lastSequence);
    id lastSeq = repl.lastSequence;
    
    AssertEqual(lastSeq, @1);
    
    Log(@"GOT DOCS: %@", [db getAllDocs:nil]);
    
    AssertEq(db.documentCount, 1u);
    AssertEq(db.lastSequenceNumber, 2);
    
    // Replicate again; should complete but add no revisions:
    Log(@"Second replication, should get no more revs:");
    replic8(db, ([self remoteTestDBURL: kScratchDBName]), NO, nil, nil, nil);
    AssertEq(db.lastSequenceNumber, 3);
    
    CBL_Revision* doc = [db getDocumentWithID: @"doc1" revisionID: nil];
    Assert(doc);
    Assert([doc.revID hasPrefix: @"2-"]);
    AssertEqual(doc[@"foo"], @1);
}
#endif


- (void) test_12_Pusher_DocIDs {
    RequireTestCase(Puller_DocIDs);
    // Create some documents:
    for (int i = 1; i <= 10; i++) {
        NSDictionary* props = @{@"_id": $sprintf(@"doc%d", i)};
        CBLStatus status;
        NSError* error;
        [db putRevision: [CBL_MutableRevision revisionWithProperties: props]
             prevRevisionID: nil allowConflict: NO status: &status error: &error];
        AssertEq(status, kCBLStatusCreated);
        AssertNil(error);
    }

    // Push them to the remote:
    NSURL* remoteDB = [self remoteTestDBURL: kScratchDBName];
    if (!remoteDB)
        return;
    [self eraseRemoteDB: remoteDB];
    replic8(db, remoteDB, YES, nil, (@[@"doc4", @"doc7"]), nil);

    // Check _all_docs on the remote db and make sure only doc4 and doc7 were pushed:
    NSURL* allDocsURL = [remoteDB URLByAppendingPathComponent: @"_all_docs"];
    NSDictionary* response = [self sendRemoteRequest: @"GET" toURL: allDocsURL];
    Assert(response);
    NSArray* rows = response[@"rows"];
    AssertEq(rows.count, 2u);
    AssertEqual((rows[0])[@"id"], @"doc4");
    AssertEqual((rows[1])[@"id"], @"doc7");
}


- (void) test_15_ParseReplicatorProperties {
    __block CBLDatabase* parsedDB = nil;
    __block NSURL* remote = nil;
    __block BOOL isPush = NO, createTarget = NO;
    __block NSDictionary* headers = nil;
    
    __block NSDictionary* props;
    props = $dict({@"source", db.name},
                  {@"target", @"http://example.com/foo"},
                  {@"create_target", $true});
    AssertEq(200, [dbmgr parseReplicatorProperties: props
                                        toDatabase: &parsedDB
                                            remote: &remote
                                            isPush: &isPush
                                      createTarget: &createTarget
                                           headers: &headers
                                        authorizer: NULL]);
    AssertEq(parsedDB, db);
    AssertEqual(remote, $url(@"http://example.com/foo"));
    AssertEq(isPush, YES);
    AssertEq(createTarget, YES);
    AssertEqual(headers, nil);
    
    props = $dict({@"source", @"cbl:///foo"},
                  {@"target", db.name});
    AssertEq(200, [dbmgr parseReplicatorProperties: props
                                             toDatabase: &parsedDB
                                                 remote: &remote
                                                 isPush: &isPush
                                           createTarget: &createTarget
                                                headers: &headers
                                             authorizer: NULL]);
    AssertEq(parsedDB, db);
    AssertEqual(remote, $url(@"cbl:///foo"));
    AssertEq(isPush, NO);
    AssertEq(createTarget, NO);
    AssertEqual(headers, nil);

    // Invalid URLs:
    [self allowWarningsIn:^{
        NSArray* badTargets = @[@"http://example.com",
                                @"http://example.com/",
                                @"http://example.com/foo?x=y",
                                @"http://example.com/foo#frag",
                                @"gopher://example.com/foo"];
        for (NSString* target in badTargets) {
            props = $dict({@"source", db.name},
                          {@"target", target});
            AssertEq(400, [dbmgr parseReplicatorProperties: props
                                                toDatabase: &parsedDB
                                                    remote: &remote
                                                    isPush: &isPush
                                              createTarget: &createTarget
                                                   headers: &headers
                                                authorizer: NULL]);
        }
    }];

    // Local-to-local replication:
    Assert(dbmgr.internalURL);
    props = $dict({@"source", db.name},
                  {@"target", @"bar"});
    AssertEq([dbmgr parseReplicatorProperties: props
                                        toDatabase: &parsedDB
                                            remote: &remote
                                            isPush: &isPush
                                      createTarget: &createTarget
                                           headers: &headers
                                        authorizer: NULL],
              404);

    CBLDatabase* barDB = [dbmgr databaseNamed: @"bar" error: NULL];
    Assert(barDB);

    AssertEq([dbmgr parseReplicatorProperties: props
                                        toDatabase: &parsedDB
                                            remote: &remote
                                            isPush: &isPush
                                      createTarget: &createTarget
                                           headers: &headers
                                        authorizer: NULL],
              200);
    AssertEq(parsedDB, db);
    AssertEqual(remote, barDB.internalURL);
    AssertEq(isPush, YES);
    AssertEqual(headers, nil);

    // OAuth:
    NSDictionary* oauthDict = $dict({@"consumer_secret", @"consumer_secret"},
                                    {@"consumer_key", @"consumer_key"},
                                    {@"token_secret", @"token_secret"},
                                    {@"token", @"token"});
    props = $dict({@"source", $dict({@"url", @"http://example.com/foo"},
                                    {@"headers", $dict({@"Excellence", @"Most"})},
                                    {@"auth", $dict({@"oauth", oauthDict})})},
                  {@"target", db.name});
    id<CBLAuthorizer> authorizer = nil;
    AssertEq(200, [dbmgr parseReplicatorProperties: props
                                             toDatabase: &parsedDB
                                                 remote: &remote
                                                 isPush: &isPush
                                           createTarget: &createTarget
                                                headers: &headers
                                             authorizer: &authorizer]);
    AssertEq(parsedDB, db);
    AssertEqual(remote, $url(@"http://example.com/foo"));
    AssertEq(isPush, NO);
    AssertEq(createTarget, NO);
    AssertEqual(headers, $dict({@"Excellence", @"Most"}));
    Assert([authorizer isKindOfClass: [CBLOAuth1Authorizer class]]);
}


- (void) test_16_FindCommonAncestor {
    NSDictionary* revDict = $dict({@"ids", @[@"second", @"first"]}, {@"start", @2});
    CBL_Revision* rev = [CBL_Revision revisionWithProperties: $dict({@"_revisions", revDict})];
    AssertEq(CBLFindCommonAncestor(rev, @[]), 0);
    AssertEq(CBLFindCommonAncestor(rev, @[@"3-noway".cbl_asRevID, @"1-nope".cbl_asRevID]), 0);
    AssertEq(CBLFindCommonAncestor(rev, @[@"3-noway".cbl_asRevID, @"1-first".cbl_asRevID]), 1);
    AssertEq(CBLFindCommonAncestor(rev, @[@"3-noway".cbl_asRevID, @"2-second".cbl_asRevID, @"1-first".cbl_asRevID]), 2);
}


- (void) test_17_Reachability {
    NSArray* hostnames = @[@"couchbase.com", @"localhost", @"127.0.0.1", @"67.221.231.37",
                           @"qqq", @"foobar.local.", @"fsdfsaf.fsdfdaf.ws"];
    for (NSString* hostname in hostnames) {
        Log(@"Test reachability of %@ ...", hostname);
        NSURL* url = $url($sprintf(@"https://%@/", hostname));
        CBLReachability* r = [[CBLReachability alloc] initWithURL: url];
        Assert(r);
        AssertEqual(r.hostName, hostname);
        __block XCTestExpectation* resolved;
        
        __weak CBLReachability *weakR = r;
        r.onChange = ^{
            CBLReachability *strongR = weakR;
            Log(@"\tonChange: known=%d, flags=%x --> reachable=%d",
                strongR.reachabilityKnown, strongR.reachabilityFlags, strongR.reachable);
            Log(@"\tCBLReachability = %@", strongR);
            if (strongR.reachabilityKnown)
                [resolved fulfill];
        };
        Assert([r startOnRunLoop:CFRunLoopGetCurrent()]);
        Log(@"\tCBLReachability = %@", r);

        BOOL known = r.reachabilityKnown;
        Log(@"\tInitially: known=%d, flags=%x --> reachable=%d",
            known, r.reachabilityFlags, r.reachable);
        if (!known) {
            resolved = [self expectationWithDescription: @"Reachability resolved"];
            [self waitForExpectationsWithTimeout: 10.0 handler: nil];
        }
        [r stop];
        Log(@"\t...done!");
    }
}


- (void) test_18_DistinctCheckpointIDs {
    NSMutableDictionary* props = [@{@"source": @"http://fake.fake/fakedb",
                                    @"target": db.name,
                                    @"continuous": @NO} mutableCopy];
    CBLStatus status;
    id<CBL_Replicator> r1 = [dbmgr replicatorWithProperties: props status: &status];
    Assert(r1);
    NSString* check1 = r1.remoteCheckpointDocID;

    props[@"continuous"] = @YES;
    id<CBL_Replicator> r2 = [dbmgr replicatorWithProperties: props status: &status];
    Assert(r2);
    NSString* check2 = r2.remoteCheckpointDocID;
    Assert(![check1 isEqualToString: check2]);

    props[@"filter"] = @"Melitta";
    props[@"query_params"] = @{@"unbleached": @"true"};     // Test fix for #728
    id<CBL_Replicator> r3 = [dbmgr replicatorWithProperties: props status: &status];
    Assert(r3);
    NSString* check3 = r3.remoteCheckpointDocID;
    Assert(![check3 isEqualToString: check2]);
}


- (void) test_19_UseRemoteUUID {   // Test kCBLReplicatorOption_RemoteUUID (see #733)
    NSDictionary* props = @{@"source": @"http://alice.local:55555/db",
                            @"target": db.name,
                            kCBLReplicatorOption_RemoteUUID: @"cafebabe"};
    CBLStatus status;
    id<CBL_Replicator> r1 = [dbmgr replicatorWithProperties: props status: &status];
    Assert(r1);
    NSString* check1 = r1.remoteCheckpointDocID;

    // Different URL, but same remoteUUID:
    NSMutableDictionary* props2 = [props mutableCopy];
    props2[@"source"] = @"http://alice17.local:44444/db";
    id<CBL_Replicator> r2 = [dbmgr replicatorWithProperties: props2 status: &status];
    Assert(r2);
    NSString* check2 = r2.remoteCheckpointDocID;
    AssertEqual(check1, check2);
    AssertEqual(r2.settings, r1.settings);

    // Same UUID but different "filter" setting:
    NSMutableDictionary* props3 = [props2 mutableCopy];
    props3[@"filter"] = @"Melitta";
    id<CBL_Replicator> r3 = [dbmgr replicatorWithProperties: props3 status: &status];
    Assert(r3);
    NSString* check3 = r3.remoteCheckpointDocID;
    Assert(![check3 isEqualToString: check2]);
    Assert(!$equal(r3.settings, r2.settings));
}


- (void) test20_PullActiveOnly {
    // Database 'attach_test' happens to have a deleted document named 'propertytest'.
    // Make sure the puller doesn't add it to an empty database:
    NSURL* remoteURL = [self remoteTestDBURL: @"attach_test"];
    if (!remoteURL)
        return;
    id lastSeq = replic8(db, remoteURL, NO, nil, nil, nil);
    AssertEqual(lastSeq, @7);
    // Ensure we didn't pull the deleted document 'propertytest':
    CBL_RevisionList* revs = [db.storage getAllRevisionsOfDocumentID: @"propertytest" onlyCurrent: NO includeDeleted: YES];
    AssertEq(revs.count, 0u);
}

- (void) test21_PullNotActiveOnly {
    // If database _isn't_ empty, the puller won't use the active_only optimization:
    [self createDocuments: 1];

    NSURL* remoteURL = [self remoteTestDBURL: @"attach_test"];
    if (!remoteURL)
        return;
    id lastSeq = replic8(db, remoteURL, NO, nil, nil, nil);
    AssertEqual(lastSeq, @7);
    // Verify we did pull the deleted document 'propertytest':
    CBL_RevisionList* revs = [db.storage getAllRevisionsOfDocumentID: @"propertytest" onlyCurrent: NO includeDeleted: YES];
    Assert(revs);
    Assert(revs[0].deleted);
}

- (void) test22_ParseAuthChallenge {
    NSDictionary* c = [BLIPHTTPLogic parseAuthHeader: nil];
    AssertNil(c);

    NSString* header = @"Basic realm=Couchbase";
    c = [BLIPHTTPLogic parseAuthHeader: header];
    AssertEqualish(c, (@{@"WWW-Authenticate": header, @"Scheme": @"Basic", @"realm": @"Couchbase"}));

    header = @"OIDC login=\"http://example.com/login?foo=bar\"";
    c = [BLIPHTTPLogic parseAuthHeader: header];
    AssertEqualish(c, (@{@"WWW-Authenticate": header, @"Scheme": @"OIDC", @"login": @"http://example.com/login?foo=bar"}));

    header = @"OIDC login=\"http://example.com/login?foo=bar\",something=other";
    c = [BLIPHTTPLogic parseAuthHeader: header];
    AssertEqualish(c, (@{@"WWW-Authenticate": header, @"Scheme": @"OIDC", @"login": @"http://example.com/login?foo=bar"}));
}


#pragma mark - UTILITY FUNCTIONS


- (NSString*) replicate: (NSURL*)remote
                   push: (BOOL)push
                 filter: (NSString*)filter
                 docIDs: (NSArray*)docIDs
            expectError: (NSError*) expectError
{
    CBL_ReplicatorSettings* settings = [[CBL_ReplicatorSettings alloc] initWithRemote: remote push: push];
    settings.filterName = filter;
    settings.docIDs = docIDs;
    settings.authorizer = self.authorizer;
    Assert([settings compilePushFilterForDatabase: db status: NULL]);
    return [self replicate: settings expectError: expectError];
}


- (NSString*) replicate: (CBL_ReplicatorSettings*)settings
            expectError: (NSError*)expectError
{
    id<CBL_Replicator> repl = [[dbmgr.replicatorClass alloc] initWithDB: db settings: settings];
    [repl start];
    
    Assert(repl.status != kCBLReplicatorStopped);
    Log(@"Waiting for replicator to finish...");
    NSDate* timeout = [NSDate dateWithTimeIntervalSinceNow: 10];
    while ((repl.status != kCBLReplicatorStopped || repl.savingCheckpoint) && timeout.timeIntervalSinceNow > 0.0) {
        if (![[NSRunLoop currentRunLoop] runMode: NSDefaultRunLoopMode
                                      beforeDate: [NSDate dateWithTimeIntervalSinceNow: 0.5]]) {
            Warn(@"Runloop exiting unexpectedly!");
            break;
        }
    }
    AssertEq(repl.status, kCBLReplicatorStopped);
    Assert(!repl.savingCheckpoint);
    if (expectError) {
        Assert($equal(repl.error.domain, expectError.domain) && repl.error.code == expectError.code,
               @"\nUnexpected error %@\n  Expected error %@",
               repl.error.my_compactDescription, expectError.my_compactDescription);
        Log(@"...replicator got expected error %@", repl.error.my_compactDescription);
    } else {
        AssertNil(repl.error);
        Log(@"...replicator finished. lastSequence=%@", repl.lastSequence);
    }
    Assert(!repl.active);
    return repl.lastSequence;
}


// Returns last sequence, or the NSError 'expectError' is non-nil
- (id) replicateContinuous: (NSURL*)remote
                      push: (BOOL)push
                    filter: (NSString*)filter
                   options: (NSDictionary*)options
               expectError: (NSError*) expectError
{
    CBL_ReplicatorSettings* settings = [[CBL_ReplicatorSettings alloc] initWithRemote: remote push: push];
    settings.continuous = YES;
    settings.authorizer = self.authorizer;
    settings.filterName = filter;
    settings.options = options;
    Assert([settings compilePushFilterForDatabase: db status: NULL]);
    id<CBL_Replicator> repl = [[dbmgr.replicatorClass alloc] initWithDB: db settings: settings];
    [repl start];

    // Start the replicator and wait for it to go active, then inactive:
    Assert(repl.status != kCBLReplicatorStopped);
    Log(@"Waiting for replicator to go idle...");
    NSDate* timeout = [NSDate dateWithTimeIntervalSinceNow: 10];
    bool wasActive = repl.active;
    BOOL stopping = NO;
    while ((repl.status != kCBLReplicatorStopped || repl.savingCheckpoint) && timeout.timeIntervalSinceNow > 0.0) {
        if (![[NSRunLoop currentRunLoop] runMode: NSDefaultRunLoopMode
                                      beforeDate: [NSDate dateWithTimeIntervalSinceNow: 0.5]])
            break;
        if (!wasActive) {
            wasActive = repl.active;
        } else if (!repl.active && !stopping) {
            if (!expectError)
                Assert(repl.status != kCBLReplicatorStopped);
            stopping = YES;
            [repl stop];  // Went inactive, so it's done; give it time to save its checkpoint
        }
    }
    Assert(wasActive && !repl.active);
    Assert(!repl.savingCheckpoint);

    if (expectError) {
        AssertEq(repl.status, kCBLReplicatorStopped);
        AssertEqual(repl.error.domain, expectError.domain);
        AssertEq(repl.error.code, expectError.code);
        Log(@"...replicator finished. error=%@", repl.error.my_compactDescription);
        return repl.error;
    } else {
        AssertNil(repl.error);
        Log(@"...replicator finished. lastSequence=%@", repl.lastSequence);
        return repl.lastSequence;
    }
}


#pragma mark - CHECKPOINT SEED FROM SIBLING:


#define kSeedListenerDBName @"checkpoint_seed_listener"
#define kSeedLiveDocCount 20
#define kSeedDeletedDocCount 10

static UInt16 sSeedListenerPort = 60500;


// The in-process listener does not implement `active_only`, so the one-shot pull receives the
// historical tombstones too. What these tests verify is the continuous replicator's starting
// sequence: seeded, it only processes the changes made after the one-shot; unseeded, it reads
// the whole feed again.
- (void) test23_ContinuousSeededFromOneShotCheckpoint {
    if (_newReplicator)
        return;     // The seed is implemented in CBLRestReplicator only.
    CBLDatabase* listenerDB;
    CBLListener* listener = [self startSeedListenerWithDatabase: &listenerDB];
    NSURL* remoteURL = [self seedListenerURLForDatabase: listenerDB listener: listener];

    NSDictionary* seedOptions = @{kCBLReplicatorOption_SeedCheckpointFromSibling: @YES};
    [self runOneShotPullFrom: remoteURL options: seedOptions];
    NSUInteger changesAfterOneShot = [self changeSeedListenerDB: listenerDB];

    id<CBL_Replicator> repl = [self runContinuousPullFrom: remoteURL options: seedOptions];
    NSString* listenerLastSequence = $sprintf(@"%lld", listenerDB.lastSequenceNumber);
    AssertEqual([repl.lastSequence description], listenerLastSequence);
    AssertEq(repl.changesTotal, changesAfterOneShot);
    [self verifyPostOneShotChangesArrived];

    [listener stop];
}


- (void) test24_ContinuousNotSeededWithoutOption {
    if (_newReplicator)
        return;
    CBLDatabase* listenerDB;
    CBLListener* listener = [self startSeedListenerWithDatabase: &listenerDB];
    NSURL* remoteURL = [self seedListenerURLForDatabase: listenerDB listener: listener];

    [self runOneShotPullFrom: remoteURL options: nil];
    [self changeSeedListenerDB: listenerDB];

    id<CBL_Replicator> repl = [self runContinuousPullFrom: remoteURL options: nil];
    NSString* listenerLastSequence = $sprintf(@"%lld", listenerDB.lastSequenceNumber);
    AssertEqual([repl.lastSequence description], listenerLastSequence);
    AssertEq(repl.changesTotal, (NSUInteger)listenerDB.documentCount + kSeedDeletedDocCount + 1);
    [self verifyPostOneShotChangesArrived];

    [listener stop];
}


- (void) test25_ContinuousNotSeededWhenSiblingCheckpointMismatches {
    if (_newReplicator)
        return;
    CBLDatabase* listenerDB;
    CBLListener* listener = [self startSeedListenerWithDatabase: &listenerDB];
    NSURL* remoteURL = [self seedListenerURLForDatabase: listenerDB listener: listener];

    NSDictionary* seedOptions = @{kCBLReplicatorOption_SeedCheckpointFromSibling: @YES};
    CBL_ReplicatorSettings* oneShotSettings = [self runOneShotPullFrom: remoteURL options: seedOptions];
    [self changeSeedListenerDB: listenerDB];

    // Make the local one-shot checkpoint disagree with the listener's copy:
    NSString* oneShotCheckpointID = [oneShotSettings remoteCheckpointDocIDForLocalUUID: db.privateUUID];
    Assert([db setLastSequence: @"1" withCheckpointID: oneShotCheckpointID]);

    id<CBL_Replicator> repl = [self runContinuousPullFrom: remoteURL options: seedOptions];
    NSString* listenerLastSequence = $sprintf(@"%lld", listenerDB.lastSequenceNumber);
    AssertEqual([repl.lastSequence description], listenerLastSequence);
    AssertEq(repl.changesTotal, (NSUInteger)listenerDB.documentCount + kSeedDeletedDocCount + 1);
    [self verifyPostOneShotChangesArrived];

    [listener stop];
}


- (void) test26_OneShotNotSeededWithoutSiblingCheckpoint {
    if (_newReplicator)
        return;
    CBLDatabase* listenerDB;
    CBLListener* listener = [self startSeedListenerWithDatabase: &listenerDB];
    NSURL* remoteURL = [self seedListenerURLForDatabase: listenerDB listener: listener];

    NSDictionary* seedOptions = @{kCBLReplicatorOption_SeedCheckpointFromSibling: @YES};
    CBL_ReplicatorSettings* settings = [[CBL_ReplicatorSettings alloc] initWithRemote: remoteURL push: NO];
    settings.options = seedOptions;
    id lastSeq = [self replicate: settings expectError: nil];
    NSString* listenerLastSequence = $sprintf(@"%lld", listenerDB.lastSequenceNumber);
    AssertEqual([lastSeq description], listenerLastSequence);
    AssertEq(db.documentCount, (unsigned)kSeedLiveDocCount);

    [listener stop];
}


- (void) test26a_ContinuousNotSeededWhenResetRequested {
    if (_newReplicator)
        return;
    CBLDatabase* listenerDB;
    CBLListener* listener = [self startSeedListenerWithDatabase: &listenerDB];
    NSURL* remoteURL = [self seedListenerURLForDatabase: listenerDB listener: listener];

    NSDictionary* seedOptions = @{kCBLReplicatorOption_SeedCheckpointFromSibling: @YES};
    [self runOneShotPullFrom: remoteURL options: seedOptions];
    [self changeSeedListenerDB: listenerDB];

    NSDictionary* seedAndResetOptions = @{kCBLReplicatorOption_SeedCheckpointFromSibling: @YES,
                                          kCBLReplicatorOption_Reset: @YES};
    id<CBL_Replicator> repl = [self runContinuousPullFrom: remoteURL options: seedAndResetOptions];
    NSString* listenerLastSequence = $sprintf(@"%lld", listenerDB.lastSequenceNumber);
    AssertEqual([repl.lastSequence description], listenerLastSequence);
    AssertEq(repl.changesTotal, (NSUInteger)listenerDB.documentCount + kSeedDeletedDocCount + 1);
    [self verifyPostOneShotChangesArrived];

    [listener stop];
}


#pragma mark - RESUME FROM LOCAL CHECKPOINT:


// The listener holds sequences 1-40 after setup (20 live docs, then 10 create+delete pairs) and
// 41-42 after -changeSeedListenerDB:. A local checkpoint rewound to 30 therefore leaves the last
// five tombstones plus the two post-checkpoint changes in the feed.
#define kRewoundLocalLastSequence @"30"
#define kChangesAfterRewoundCheckpoint 7

- (void) test27_ResumesFromLocalCheckpointOnMismatch {
    if (_newReplicator)
        return;     // Implemented in CBLRestReplicator only.
    CBLDatabase* listenerDB;
    CBLListener* listener = [self startSeedListenerWithDatabase: &listenerDB];
    NSURL* remoteURL = [self seedListenerURLForDatabase: listenerDB listener: listener];
    NSDictionary* resumeOptions = @{kCBLReplicatorOption_ResumeFromLocalCheckpoint: @YES};

    id<CBL_Replicator> firstRepl = [self runContinuousPullFrom: remoteURL options: resumeOptions];
    NSString* checkpointID = firstRepl.remoteCheckpointDocID;
    NSString* remoteLastSequence = [firstRepl.lastSequence description];
    NSString* listenerLastSequence = $sprintf(@"%lld", listenerDB.lastSequenceNumber);
    AssertEqual(remoteLastSequence, listenerLastSequence);
    [self changeSeedListenerDB: listenerDB];
    Assert([db setLastSequence: kRewoundLocalLastSequence withCheckpointID: checkpointID]);

    NSDictionary* mismatch = [self captureCheckpointMismatchWhileRunning: ^id<CBL_Replicator> {
        return [self runContinuousPullFrom: remoteURL options: resumeOptions];
    }];
    AssertEqual(mismatch[CBL_ReplicatorCheckpointMismatchLocalLastSequenceKey],
                kRewoundLocalLastSequence);
    AssertEqual(mismatch[CBL_ReplicatorCheckpointMismatchRemoteLastSequenceKey], remoteLastSequence);
    AssertEqual(mismatch[CBL_ReplicatorCheckpointMismatchResumedFromLocalKey], @YES);
    AssertEq([mismatch[@"changesTotal"] unsignedIntegerValue], (NSUInteger)kChangesAfterRewoundCheckpoint);
    listenerLastSequence = $sprintf(@"%lld", listenerDB.lastSequenceNumber);
    AssertEqual(mismatch[@"lastSequence"], listenerLastSequence);
    AssertEqual([db lastSequenceWithCheckpointID: checkpointID], listenerLastSequence);
    [self verifyPostOneShotChangesArrived];

    [listener stop];
}


- (void) test28_ReplicatesFromScratchOnMismatchWithoutOption {
    if (_newReplicator)
        return;
    CBLDatabase* listenerDB;
    CBLListener* listener = [self startSeedListenerWithDatabase: &listenerDB];
    NSURL* remoteURL = [self seedListenerURLForDatabase: listenerDB listener: listener];

    id<CBL_Replicator> firstRepl = [self runContinuousPullFrom: remoteURL options: nil];
    NSString* checkpointID = firstRepl.remoteCheckpointDocID;
    NSString* remoteLastSequence = [firstRepl.lastSequence description];
    [self changeSeedListenerDB: listenerDB];
    Assert([db setLastSequence: kRewoundLocalLastSequence withCheckpointID: checkpointID]);

    NSDictionary* mismatch = [self captureCheckpointMismatchWhileRunning: ^id<CBL_Replicator> {
        return [self runContinuousPullFrom: remoteURL options: nil];
    }];
    AssertEqual(mismatch[CBL_ReplicatorCheckpointMismatchLocalLastSequenceKey],
                kRewoundLocalLastSequence);
    AssertEqual(mismatch[CBL_ReplicatorCheckpointMismatchRemoteLastSequenceKey], remoteLastSequence);
    AssertEqual(mismatch[CBL_ReplicatorCheckpointMismatchResumedFromLocalKey], @NO);
    AssertEq([mismatch[@"changesTotal"] unsignedIntegerValue],
             (NSUInteger)listenerDB.documentCount + kSeedDeletedDocCount + 1);
    NSString* listenerLastSequence = $sprintf(@"%lld", listenerDB.lastSequenceNumber);
    AssertEqual(mismatch[@"lastSequence"], listenerLastSequence);
    [self verifyPostOneShotChangesArrived];

    [listener stop];
}


- (void) test29_ReplicatesFromScratchWhenRemoteCheckpointIsMissing {
    if (_newReplicator)
        return;
    CBLDatabase* listenerDB;
    CBLListener* listener = [self startSeedListenerWithDatabase: &listenerDB];
    NSURL* remoteURL = [self seedListenerURLForDatabase: listenerDB listener: listener];
    NSDictionary* resumeOptions = @{kCBLReplicatorOption_ResumeFromLocalCheckpoint: @YES};

    id<CBL_Replicator> firstRepl = [self runContinuousPullFrom: remoteURL options: resumeOptions];
    NSString* checkpointID = firstRepl.remoteCheckpointDocID;
    NSString* localLastSequence = [db lastSequenceWithCheckpointID: checkpointID];
    AssertEqual(localLastSequence, [firstRepl.lastSequence description]);
    [self changeSeedListenerDB: listenerDB];
    Assert([listenerDB deleteLocalDocumentWithID: checkpointID error: NULL]);

    NSDictionary* mismatch = [self captureCheckpointMismatchWhileRunning: ^id<CBL_Replicator> {
        return [self runContinuousPullFrom: remoteURL options: resumeOptions];
    }];
    AssertEqual(mismatch[CBL_ReplicatorCheckpointMismatchLocalLastSequenceKey], localLastSequence);
    AssertNil(mismatch[CBL_ReplicatorCheckpointMismatchRemoteLastSequenceKey]);
    AssertEqual(mismatch[CBL_ReplicatorCheckpointMismatchResumedFromLocalKey], @NO);
    AssertEq([mismatch[@"changesTotal"] unsignedIntegerValue],
             (NSUInteger)listenerDB.documentCount + kSeedDeletedDocCount + 1);
    [self verifyPostOneShotChangesArrived];

    [listener stop];
}


// A remote checkpoint behind the local one is what a save interrupted during its PUT leaves
// behind (the local checkpoint is written first). The pending-remote mark written alongside it
// names the remote sequence that PUT was replacing; when the remote still holds that sequence
// the save was merely interrupted and the replicator resumes from the local checkpoint.
#define kStaleRemoteLastSequence @"20"

- (void) test30_ResumesWhenRemoteCheckpointIsBehindLocalAfterInterruptedSave {
    if (_newReplicator)
        return;
    CBLDatabase* listenerDB;
    CBLListener* listener = [self startSeedListenerWithDatabase: &listenerDB];
    NSURL* remoteURL = [self seedListenerURLForDatabase: listenerDB listener: listener];
    NSDictionary* resumeOptions = @{kCBLReplicatorOption_ResumeFromLocalCheckpoint: @YES};

    id<CBL_Replicator> firstRepl = [self runContinuousPullFrom: remoteURL options: resumeOptions];
    NSString* checkpointID = firstRepl.remoteCheckpointDocID;
    AssertNil([db pendingRemoteLastSequenceWithCheckpointID: checkpointID]);
    [self changeSeedListenerDB: listenerDB];
    Assert([db setLastSequence: kRewoundLocalLastSequence withCheckpointID: checkpointID]);
    Assert([db setPendingRemoteLastSequence: kStaleRemoteLastSequence withCheckpointID: checkpointID]);
    Assert([listenerDB putLocalDocument: @{@"lastSequence": kStaleRemoteLastSequence}
                                 withID: checkpointID
                                  error: NULL]);

    NSDictionary* mismatch = [self captureCheckpointMismatchWhileRunning: ^id<CBL_Replicator> {
        return [self runContinuousPullFrom: remoteURL options: resumeOptions];
    }];
    AssertEqual(mismatch[CBL_ReplicatorCheckpointMismatchLocalLastSequenceKey],
                kRewoundLocalLastSequence);
    AssertEqual(mismatch[CBL_ReplicatorCheckpointMismatchRemoteLastSequenceKey],
                kStaleRemoteLastSequence);
    AssertEqual(mismatch[CBL_ReplicatorCheckpointMismatchResumedFromLocalKey], @YES);
    AssertEq([mismatch[@"changesTotal"] unsignedIntegerValue], (NSUInteger)kChangesAfterRewoundCheckpoint);
    NSString* listenerLastSequence = $sprintf(@"%lld", listenerDB.lastSequenceNumber);
    AssertEqual(mismatch[@"lastSequence"], listenerLastSequence);
    AssertEqual([db lastSequenceWithCheckpointID: checkpointID], listenerLastSequence);
    AssertNil([db pendingRemoteLastSequenceWithCheckpointID: checkpointID]);
    [self verifyPostOneShotChangesArrived];

    [listener stop];
}


- (void) test31_ReplicatesFromScratchWhenRemoteCheckpointIsRolledBack {
    if (_newReplicator)
        return;
    CBLDatabase* listenerDB;
    CBLListener* listener = [self startSeedListenerWithDatabase: &listenerDB];
    NSURL* remoteURL = [self seedListenerURLForDatabase: listenerDB listener: listener];
    NSDictionary* resumeOptions = @{kCBLReplicatorOption_ResumeFromLocalCheckpoint: @YES};

    id<CBL_Replicator> firstRepl = [self runContinuousPullFrom: remoteURL options: resumeOptions];
    NSString* checkpointID = firstRepl.remoteCheckpointDocID;
    [self changeSeedListenerDB: listenerDB];
    Assert([db setLastSequence: kRewoundLocalLastSequence withCheckpointID: checkpointID]);
    Assert([listenerDB putLocalDocument: @{@"lastSequence": kStaleRemoteLastSequence}
                                 withID: checkpointID
                                  error: NULL]);

    NSDictionary* mismatch = [self captureCheckpointMismatchWhileRunning: ^id<CBL_Replicator> {
        return [self runContinuousPullFrom: remoteURL options: resumeOptions];
    }];
    AssertEqual(mismatch[CBL_ReplicatorCheckpointMismatchLocalLastSequenceKey],
                kRewoundLocalLastSequence);
    AssertEqual(mismatch[CBL_ReplicatorCheckpointMismatchRemoteLastSequenceKey],
                kStaleRemoteLastSequence);
    AssertEqual(mismatch[CBL_ReplicatorCheckpointMismatchResumedFromLocalKey], @NO);
    AssertEq([mismatch[@"changesTotal"] unsignedIntegerValue],
             (NSUInteger)listenerDB.documentCount + kSeedDeletedDocCount + 1);
    NSString* listenerLastSequence = $sprintf(@"%lld", listenerDB.lastSequenceNumber);
    AssertEqual(mismatch[@"lastSequence"], listenerLastSequence);
    [self verifyPostOneShotChangesArrived];

    [listener stop];
}


- (void) test32_ResumesWhenRemoteCheckpointIsMissingAfterInterruptedFirstSave {
    if (_newReplicator)
        return;
    CBLDatabase* listenerDB;
    CBLListener* listener = [self startSeedListenerWithDatabase: &listenerDB];
    NSURL* remoteURL = [self seedListenerURLForDatabase: listenerDB listener: listener];
    NSDictionary* resumeOptions = @{kCBLReplicatorOption_ResumeFromLocalCheckpoint: @YES};

    id<CBL_Replicator> firstRepl = [self runContinuousPullFrom: remoteURL options: resumeOptions];
    NSString* checkpointID = firstRepl.remoteCheckpointDocID;
    NSString* localLastSequence = [db lastSequenceWithCheckpointID: checkpointID];
    NSUInteger changesAfterFirstRun = [self changeSeedListenerDB: listenerDB];
    Assert([db setPendingRemoteLastSequence: kCBLPendingRemoteCheckpointMissing
                           withCheckpointID: checkpointID]);
    Assert([listenerDB deleteLocalDocumentWithID: checkpointID error: NULL]);

    NSDictionary* mismatch = [self captureCheckpointMismatchWhileRunning: ^id<CBL_Replicator> {
        return [self runContinuousPullFrom: remoteURL options: resumeOptions];
    }];
    AssertEqual(mismatch[CBL_ReplicatorCheckpointMismatchLocalLastSequenceKey], localLastSequence);
    AssertNil(mismatch[CBL_ReplicatorCheckpointMismatchRemoteLastSequenceKey]);
    AssertEqual(mismatch[CBL_ReplicatorCheckpointMismatchResumedFromLocalKey], @YES);
    AssertEq([mismatch[@"changesTotal"] unsignedIntegerValue], changesAfterFirstRun);
    AssertNil([db pendingRemoteLastSequenceWithCheckpointID: checkpointID]);
    [self verifyPostOneShotChangesArrived];

    [listener stop];
}


// Runs the replicator produced by `block` while observing CBL_ReplicatorCheckpointMismatchNotification.
// Returns the notification's userInfo extended with the finished replicator's "changesTotal" and
// "lastSequence", and asserts that exactly one mismatch was posted.
- (NSDictionary*) captureCheckpointMismatchWhileRunning: (id<CBL_Replicator> (^)(void))block {
    NSMutableArray* mismatches = [NSMutableArray array];
    id observer = [[NSNotificationCenter defaultCenter]
                    addObserverForName: CBL_ReplicatorCheckpointMismatchNotification
                                object: nil
                                 queue: nil
                            usingBlock: ^(NSNotification* n) {
                                [mismatches addObject: n.userInfo];
                            }];
    id<CBL_Replicator> repl = block();
    [[NSNotificationCenter defaultCenter] removeObserver: observer];

    AssertEq(mismatches.count, 1u);
    NSMutableDictionary* result = [mismatches[0] mutableCopy];
    result[@"changesTotal"] = @(repl.changesTotal);
    [result setValue: [repl.lastSequence description] forKey: @"lastSequence"];
    return result;
}


#pragma mark - TOMBSTONES OF UNKNOWN DOCUMENTS:


static NSDictionary* skipTombstonesOptions(void) {
    return @{kCBLReplicatorOption_SkipTombstonesOfUnknownDocuments: @YES};
}


// The listener's ten "deleted-N" docs were created and deleted before the local db ever saw them.
- (void) test33_PullSkipsTombstonesOfUnknownDocuments {
    if (_newReplicator)
        return;     // Implemented in CBLRestPuller only.
    CBLDatabase* listenerDB;
    CBLListener* listener = [self startSeedListenerWithDatabase: &listenerDB];
    NSURL* remoteURL = [self seedListenerURLForDatabase: listenerDB listener: listener];

    CBL_ReplicatorSettings* settings = [[CBL_ReplicatorSettings alloc] initWithRemote: remoteURL push: NO];
    settings.options = skipTombstonesOptions();
    id<CBL_Replicator> repl = [self runReplicator: settings];
    NSString* listenerLastSequence = $sprintf(@"%lld", listenerDB.lastSequenceNumber);
    AssertEqual([repl.lastSequence description], listenerLastSequence);
    AssertEq(repl.changesProcessed, repl.changesTotal);
    AssertEq(repl.changesTotal, (NSUInteger)(kSeedLiveDocCount + kSeedDeletedDocCount));
    AssertEq(db.documentCount, (unsigned)kSeedLiveDocCount);
    [self assertStatusOfSeedDeletedDocuments: kCBLStatusNotFound];

    [listener stop];
}


- (void) test34_PullKeepsTombstonesWithoutOption {
    if (_newReplicator)
        return;
    CBLDatabase* listenerDB;
    CBLListener* listener = [self startSeedListenerWithDatabase: &listenerDB];
    NSURL* remoteURL = [self seedListenerURLForDatabase: listenerDB listener: listener];

    [self runOneShotPullFrom: remoteURL options: nil];
    [self assertStatusOfSeedDeletedDocuments: kCBLStatusDeleted];

    [listener stop];
}


// A deletion of a document the local db already has must still be pulled.
- (void) test35_PullKeepsTombstonesOfKnownDocuments {
    if (_newReplicator)
        return;
    CBLDatabase* listenerDB;
    CBLListener* listener = [self startSeedListenerWithDatabase: &listenerDB];
    NSURL* remoteURL = [self seedListenerURLForDatabase: listenerDB listener: listener];

    [self runOneShotPullFrom: remoteURL options: skipTombstonesOptions()];
    [self changeSeedListenerDB: listenerDB];
    [self runOneShotPullFrom: remoteURL options: skipTombstonesOptions()];
    [self verifyPostOneShotChangesArrived];

    [listener stop];
}


- (void) test36_PushSkipsTombstonesOfUnknownDocuments {
    if (_newReplicator)
        return;     // Implemented in CBLRestPusher only.
    CBLDatabase* listenerDB;
    CBLListener* listener = [self startSeedListenerWithDatabase: &listenerDB];
    NSURL* remoteURL = [self seedListenerURLForDatabase: listenerDB listener: listener];
    [self createLocalLiveDocumentAndTombstone];

    CBL_ReplicatorSettings* settings = [[CBL_ReplicatorSettings alloc] initWithRemote: remoteURL push: YES];
    settings.options = skipTombstonesOptions();
    id<CBL_Replicator> repl = [self runReplicator: settings];
    NSString* localLastSequence = $sprintf(@"%lld", db.lastSequenceNumber);
    AssertEqual([repl.lastSequence description], localLastSequence);
    AssertEq([self statusOfDocument: @"local-live" in: listenerDB], kCBLStatusOK);
    AssertEq([self statusOfDocument: @"local-deleted" in: listenerDB], kCBLStatusNotFound);

    [listener stop];
}


- (void) test37_PushKeepsTombstonesWithoutOption {
    if (_newReplicator)
        return;
    CBLDatabase* listenerDB;
    CBLListener* listener = [self startSeedListenerWithDatabase: &listenerDB];
    NSURL* remoteURL = [self seedListenerURLForDatabase: listenerDB listener: listener];
    [self createLocalLiveDocumentAndTombstone];

    CBL_ReplicatorSettings* settings = [[CBL_ReplicatorSettings alloc] initWithRemote: remoteURL push: YES];
    [self runReplicator: settings];
    AssertEq([self statusOfDocument: @"local-live" in: listenerDB], kCBLStatusOK);
    AssertEq([self statusOfDocument: @"local-deleted" in: listenerDB], kCBLStatusDeleted);

    [listener stop];
}


// A deletion of a document the remote already has must still be pushed.
- (void) test38_PushKeepsTombstonesOfKnownDocuments {
    if (_newReplicator)
        return;
    CBLDatabase* listenerDB;
    CBLListener* listener = [self startSeedListenerWithDatabase: &listenerDB];
    NSURL* remoteURL = [self seedListenerURLForDatabase: listenerDB listener: listener];
    [self createLocalLiveDocumentAndTombstone];

    CBL_ReplicatorSettings* settings = [[CBL_ReplicatorSettings alloc] initWithRemote: remoteURL push: YES];
    settings.options = skipTombstonesOptions();
    [self runReplicator: settings];
    AssertEq([self statusOfDocument: @"local-live" in: listenerDB], kCBLStatusOK);

    Assert([db[@"local-live"] deleteDocument: NULL]);
    [self runReplicator: settings];
    AssertEq([self statusOfDocument: @"local-live" in: listenerDB], kCBLStatusDeleted);

    [listener stop];
}


- (void) createLocalLiveDocumentAndTombstone {
    CBLUnsavedRevision* liveRev = db[@"local-live"].newRevision;
    liveRev[@"index"] = @1;
    Assert([liveRev save: NULL]);
    CBLDocument* deletedDoc = db[@"local-deleted"];
    CBLUnsavedRevision* deletedRev = deletedDoc.newRevision;
    deletedRev[@"index"] = @2;
    Assert([deletedRev save: NULL]);
    Assert([deletedDoc deleteDocument: NULL]);
}


- (CBLStatus) statusOfDocument: (NSString*)docID in: (CBLDatabase*)database {
    CBLStatus status;
    [database.storage getDocumentWithID: docID revisionID: nil withBody: NO status: &status];
    return status;
}


- (void) assertStatusOfSeedDeletedDocuments: (CBLStatus)expectedStatus {
    for (int i = 1; i <= kSeedDeletedDocCount; i++) {
        NSString* docID = $sprintf(@"deleted-%d", i);
        CBLStatus status = [self statusOfDocument: docID in: db];
        AssertEq(status, expectedStatus);
    }
}


// Like -replicate:expectError: but returns the finished replicator so its counters can be checked.
- (id<CBL_Replicator>) runReplicator: (CBL_ReplicatorSettings*)settings {
    id<CBL_Replicator> repl = [[dbmgr.replicatorClass alloc] initWithDB: db settings: settings];
    [repl start];
    Assert(repl.status != kCBLReplicatorStopped);
    NSDate* timeout = [NSDate dateWithTimeIntervalSinceNow: 10];
    while ((repl.status != kCBLReplicatorStopped || repl.savingCheckpoint) && timeout.timeIntervalSinceNow > 0.0) {
        if (![[NSRunLoop currentRunLoop] runMode: NSDefaultRunLoopMode
                                      beforeDate: [NSDate dateWithTimeIntervalSinceNow: 0.5]])
            break;
    }
    AssertEq(repl.status, kCBLReplicatorStopped);
    Assert(!repl.savingCheckpoint);
    AssertNil(repl.error);
    Assert(!repl.active);
    return repl;
}


- (CBLListener*) startSeedListenerWithDatabase: (CBLDatabase**)outListenerDB {
    NSError* error;
    CBLDatabase* listenerDB = [dbmgr databaseNamed: kSeedListenerDBName error: &error];
    Assert(listenerDB, @"Couldn't create listener db: %@", error);
    [listenerDB inTransaction: ^BOOL{
        for (int i = 1; i <= kSeedLiveDocCount; i++) {
            CBLUnsavedRevision* rev = listenerDB[$sprintf(@"live-%d", i)].newRevision;
            rev[@"index"] = @(i);
            Assert([rev save: NULL]);
        }
        for (int i = 1; i <= kSeedDeletedDocCount; i++) {
            CBLDocument* doc = listenerDB[$sprintf(@"deleted-%d", i)];
            CBLUnsavedRevision* rev = doc.newRevision;
            rev[@"index"] = @(i);
            Assert([rev save: NULL]);
            Assert([doc deleteDocument: NULL]);
        }
        return YES;
    }];

    UInt16 port = sSeedListenerPort++;
    CBLListener* listener = [[CBLHTTPListener alloc] initWithManager: dbmgr port: port];
    [self keyValueObservingExpectationForObject: listener keyPath: @"port" expectedValue: @(port)];
    Assert([listener start: &error], @"Couldn't start listener: %@", error);
    [self waitForExpectationsWithTimeout: 5.0 handler: nil];
    *outListenerDB = listenerDB;
    return listener;
}


// The host app's ATS exception for ".local" blocks plain HTTP to listener.URL; localhost is allowed.
- (NSURL*) seedListenerURLForDatabase: (CBLDatabase*)listenerDB listener: (CBLListener*)listener {
    return [NSURL URLWithString: $sprintf(@"http://localhost:%u/%@", listener.port, listenerDB.name)];
}


// Deletes one live document and creates one; returns the number of changes the feed now holds
// after the one-shot checkpoint.
- (NSUInteger) changeSeedListenerDB: (CBLDatabase*)listenerDB {
    Assert([listenerDB[@"live-1"] deleteDocument: NULL]);
    CBLUnsavedRevision* rev = listenerDB[@"created-after-one-shot"].newRevision;
    rev[@"index"] = @(kSeedLiveDocCount + 1);
    Assert([rev save: NULL]);
    return 2;
}


- (void) verifyPostOneShotChangesArrived {
    CBL_RevisionList* deletedAfterOneShot = [db.storage getAllRevisionsOfDocumentID: @"live-1"
                                                                        onlyCurrent: YES
                                                                     includeDeleted: YES];
    AssertEq(deletedAfterOneShot.count, 1u);
    Assert(deletedAfterOneShot[0].deleted);
    Assert([db getDocumentWithID: @"created-after-one-shot" revisionID: nil]);
}


- (CBL_ReplicatorSettings*) runOneShotPullFrom: (NSURL*)remoteURL options: (NSDictionary*)options {
    CBL_ReplicatorSettings* settings = [[CBL_ReplicatorSettings alloc] initWithRemote: remoteURL push: NO];
    settings.options = options;
    [self replicate: settings expectError: nil];
    AssertEq(db.documentCount, (unsigned)kSeedLiveDocCount);
    return settings;
}


- (id<CBL_Replicator>) runContinuousPullFrom: (NSURL*)remoteURL options: (NSDictionary*)options {
    CBL_ReplicatorSettings* settings = [[CBL_ReplicatorSettings alloc] initWithRemote: remoteURL push: NO];
    settings.continuous = YES;
    settings.options = options;
    id<CBL_Replicator> repl = [[dbmgr.replicatorClass alloc] initWithDB: db settings: settings];
    [repl start];

    Assert(repl.status != kCBLReplicatorStopped);
    NSDate* timeout = [NSDate dateWithTimeIntervalSinceNow: 10];
    bool wasActive = repl.active;
    BOOL stopping = NO;
    while ((repl.status != kCBLReplicatorStopped || repl.savingCheckpoint) && timeout.timeIntervalSinceNow > 0.0) {
        if (![[NSRunLoop currentRunLoop] runMode: NSDefaultRunLoopMode
                                      beforeDate: [NSDate dateWithTimeIntervalSinceNow: 0.5]])
            break;
        if (!wasActive) {
            wasActive = repl.active;
        } else if (!repl.active && !stopping) {
            stopping = YES;
            [repl stop];
        }
    }
    Assert(wasActive && !repl.active);
    Assert(!repl.savingCheckpoint);
    AssertNil(repl.error);
    return repl;
}


@end
