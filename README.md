# P2PFileSharing
A local peer-to-peer file sharing application developed as a student project for DS1

## Messages
### Structure
|control-byte|uuid|id|length|content|
|------------|----|--|------|-------|
|1 byte|16 byte|16 byte|4 byte|?|
### Types
|name|control-byte|length|content|
|----|------------|------|-------|
|init|0|8|IP(4byte)+port(int)|
|identify|1|0|-|
|disconnect|2|16|uuid|
|heartbeat|3|0|-|
|register|10|20+?|hash(20byte)+name(?)|
|de-register|10|20|hash(20byte)|
|election|20|0|-|
|leader|22|16|leader-uuid|
