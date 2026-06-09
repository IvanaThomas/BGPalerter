import fs from "node:fs";
import readline from "node:readline";
import Connector from "./connector.js";
import {AS, Path} from "../model";

const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));

export default class ConnectorNDJSON extends Connector {
    constructor(name, params, env) {
        super(name, params, env);
        this.file = params.file;
        this.stopped = false;
        this.reader = null;
        this.speed = params.speed || 1;
        this.timedReplay = params.timedReplay || false;
    }

    // must implement static transform
    // will transform message from ndjson to what bgpalerter wants
    static transform = (message) => {
        console.log("TRANSFORM CALLED", message.type);
        if (message.type !== "ndjson_message") {
            return [];
        }
        const record = message.data;
        console.log("TRANSFORM RECORD", record.prefix, record.origin )

        const components = [];
        const timestamp = Number(record.timestamp ) * 1000;


        if (record.type === "ANNOUNCE") {
            const path = new Path(record.as_path.split(/\s+/).map(i => new AS(Number(i))));
            const originAS = path.getLast();
            components.push( {
                type: "announcement",
                prefix: record.prefix,
                peer: record.peer_ip,
                peerAS: Number(record.peer_asn),
                path,
                originAS,
                nextHop: record.next_hop,
                timestamp,
                communities: []
            });
        }
        if (record.type === "WITHDRAW") {
            components.push({
                type: "withdrawal",
                prefix: record.prefix,
                peer: record.peer_ip,
                peerAS: record.peer_asn,
                timestamp
            });
        }
        console.log("TRANSFORMED COMPONENTS", components);
        return components;
    };

    // must implement connect 
    //will check that this.params.file exists, call this._connect("message") and resolve true
    connect = () => new Promise((resolve, reject) => {
        if (!this.file) {
            return reject(new Error("missing params.file for connectorNDJSON"));
        }
        if (!fs.existsSync(this.file)) {
            return reject(new Error(`NDJSON file not found: ${this.file}`));
        }
        this._connect(`NDJSON connector ready ${this.file}`);
        resolve(true);
    });

    // must implement subscribe
    // open and read NDJSON file, parse it as json, transform it, emit it using _message()
    subscribe = async () => {
        try {
            const stream = fs.createReadStream(this.file, {encoding: "utf8"});

            this.reader = readline.createInterface({
                input: stream,
                crlfDelay: Infinity
            });

            let previousTimestamp = null;

            for await (const line of this.reader) {
                if (this.stopped) break;
                if (!line.trim()) continue;

                let record;

                try {
                    record = JSON.parse(line);
                    console.log("parsed record", record);
                } catch(error) {
                    this._error(`Invalid NDJSON line: ${error.message}`);
                    continue;
                }
                const currentTimestamp = Number(record.timestamp);

                if (this.timedReplay && previousTimestamp !== null) {
                    const deltaseconds = currentTimestamp - previousTimestamp;
                    const delayms = Math.max(0, (deltaseconds * 1000) / this.speed);

                    if (delayms > 0) {
                        console.log(`Waiting ${delayms} ms before next event`);
                        await sleep(delayms);
                    }
                }

                previousTimestamp = currentTimestamp;

                console.log("EMITTING raw NDJSON record", record.prefix);

                this._message({
                    type: "ndjson_message",
                    data: record
                });
            }
            this._disconnect("NDJSON replay finished");
        } catch (error) {
            this._error(error);
        }
    };

    disconnect = () => {

        this.stopped = true;
        if(this.reader) {
            this.reader.close();
        }
        this._disconnect('NDJSON connector disconnected');

    };
}