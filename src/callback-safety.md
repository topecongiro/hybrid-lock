## Safety

The callback must not convert the given pointer to a reference. Such conversion
will violate the alias ruling when there is a mutable reference from the exclusive 
lock ownver.