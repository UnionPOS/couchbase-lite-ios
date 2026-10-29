//
//  CBL_SQLiteViewStorage.h
//  CouchbaseLite
//
//  Created by Jens Alfke on 1/16/15.
//  Copyright (c) 2015 Couchbase, Inc. All rights reserved.
//

#import "CBL_ViewStorage.h"
@class CBL_SQLiteStorage;


/** When enabled, each view remembers the sequence at which its index last gained or lost rows, so
    `lastSequenceChangedAt` can tell live queries that the view is unchanged. Off by default: then
    `lastSequenceChangedAt` equals `lastSequenceIndexed`, as in upstream Couchbase Lite 1.4. */
BOOL CBL_SQLiteViewStorageTracksIndexChanges(void);
void CBL_SQLiteViewStorageSetTracksIndexChanges(BOOL tracks);


@interface CBL_SQLiteViewStorage : NSObject <CBL_ViewStorage, CBL_QueryRowStorage>

- (instancetype) initWithDBStorage: (CBL_SQLiteStorage*)dbStorage
                              name: (NSString*)name
                            create: (BOOL)create;

@end
