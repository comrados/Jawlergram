## Crawlergram

Java crawler for Telegram based on [TelegramApi](https://github.com/rubenlagus/TelegramApi) by rubenlagus.

Collected data is stored in DB. At the moment only MongoDB storage is implemented.
You can implement other storages using the DBStorage interface.

## Dependencies

1. Java 1.8+.

2. [TelegramApi 66+](https://github.com/onixred/TelegramApi) library.

3. [MongoDB Java Driver 3+](https://github.com/mongodb/mongo-java-driver) library.

JARs can be found in /lib.

## How to use

### Normal syntax

Syntax: `java -jar Jawlergram.jar <run_type> <api_config> <crawler_cfg> <storage_config>`

* `<run_type>` - type of the launch
* `<api_config>` - api configuration file
* `<crawler_cfg>` - crawler configuration file
* `<storage_config>` - storage configuration file

Example : `java -jar Jawlergram.jar 3 api.cfg crawler.cfg storage.cfg`

### Run types

`<run_type>` argument must be an integer.

`1` - Saves only messages to DB.

`2` - Saves messages to DB, files to HDD.

`3` - Saves messages to DB, files to DB.

`4` - Saves only files to DB.

`5` - Saves only files to HDD.

`6` - Saves only voice messages to HDD.

`7` - Saves only messages to HDD.

## Acknowledgments

  * To [Rubenlagus](https://github.com/rubenlagus) and [Onixred](https://github.com/onixred) for  [TelegramApi](https://github.com/onixred/TelegramApi).
  * To [Lonami](https://github.com/Lonami) for [Telethon](https://github.com/LonamiWebs/Telethon) library and documentation, which was used for clarifications in some tricky cases.

## License

MIT License

Copyright (c) 2018 Georgii Mikriukov

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
