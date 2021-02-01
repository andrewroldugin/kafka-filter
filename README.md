# kafka-filter

## Usage

### How to run kafka On Linux

We need to start up a Kafka “broker”, a server running Kafka. The easiest way to do this is to spin up a landoop/fast-data-dev Docker container. You can do this by running the following command (you’ll need Docker installed):

``` shell
docker run --rm --net=host landoop/fast-data-dev
```

### How to run web service

``` shell
lein run
```

## License

Copyright © 2021 FIXME

This program and the accompanying materials are made available under the
terms of the Eclipse Public License 2.0 which is available at
http://www.eclipse.org/legal/epl-2.0.

This Source Code may also be made available under the following Secondary
Licenses when the conditions for such availability set forth in the Eclipse
Public License, v. 2.0 are satisfied: GNU General Public License as published by
the Free Software Foundation, either version 2 of the License, or (at your
option) any later version, with the GNU Classpath Exception which is available
at https://www.gnu.org/software/classpath/license.html.
