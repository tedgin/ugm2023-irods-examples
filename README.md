# ugm2023-irods-examples

These are example rule logic for iRODS UGM 2023 talk.

The example consists of PYthon rule logic written for iRODS 4.2.12. The rules enforce the following two policies.

1. A checksum is stored for each replica of each data object.

2. Each data object has two up-to-date replicas as long as the data object has no replicas with invalid checksums.
