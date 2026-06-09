use bgpkit_parser::BgpkitParser;
use bgpkit_broker;
use serde_json::{json, Value};
use std::fs::File;
use std::io::Write;
use clap::Parser;
use std::collections::BTreeSet;

#[derive(Parser, Debug)]
#[command(about = "Fetch bgp routes")]
struct Args {
    #[arg(short, long, default_value_t = 7960)]
    asn: u32,

    //start default 2025-01-01
    #[arg(short = 's', long, default_value = "1735689600")]
    start: String,

    //end default 2025-12-01
    #[arg(short = 'e', long, default_value = "1764547200")]
    end: String,

    #[arg(short = 'n', long, default_value_t = 50)]
    num_records: usize,

}

fn main() {
    let args = Args::parse();

    let mut file = File::create("output.ndjson").unwrap();
    let mut records: Vec<Value> = Vec::new();
    let mut discovered_prefixes: BTreeSet<String> = BTreeSet::new();

    let broker = bgpkit_broker::BgpkitBroker::new()
        .ts_start(&args.start)
        .ts_end(&args.end)
        .page(1);
     
    for item in broker.into_iter().take(1) {
        println!("found broker item: {}", item.url);

        let parser = BgpkitParser::new(item.url.as_str()).unwrap();

        for elem in parser.into_elem_iter(){
            if let Some(origin) = &elem.origin_asns {
                if origin.contains(&args.asn.into()) {

                    let record = json!({
                        "prefix": elem.prefix.to_string(),
                        "type": format!("{:?}", elem.elem_type),
                        "peer_ip": elem.peer_ip.to_string(),
                        "peer_asn": elem.peer_asn.to_string(),
                        "next_hop": elem.next_hop.map(|ip| ip.to_string()),
                        "as_path": elem.as_path.as_ref().map(|p| p.to_string()),
                        "origin": origin.iter().map(|a| a.to_string()).collect::<Vec<_>>(),
                        "timestamp": elem.timestamp
                    });

                    records.push(record);
                    discovered_prefixes.insert(elem.prefix.to_string());

                    if records.len() >= args.num_records {
                        break;
                    }
                }

            }
        }
        if records.len() >= args.num_records {
            break;
        }

    }

    records.sort_by(|a, b| {
            let ta = a["timestamp"].as_f64().unwrap_or(0.0);
            let tb = b["timestamp"].as_f64().unwrap_or(0.0);
            ta.partial_cmp(&tb).unwrap()});
    for record in &records {
        writeln!(file, "{}", record).unwrap();
    }
    println!("wrote {} records to output.ndjson", records.len());

    //write prefixes.yml
    let mut prefixes_file = File::create("prefixes.yml").unwrap();
    for prefix in &discovered_prefixes {
        writeln!(prefixes_file, "{}:", prefix).unwrap();
        writeln!(prefixes_file, "  description: Auto-discovered prefix for AS{}", args.asn).unwrap();
        writeln!(prefixes_file, "  asn:").unwrap();
        writeln!(prefixes_file, "    - {}", args.asn).unwrap();
        writeln!(prefixes_file, "  ignoreMorespecifics: false").unwrap();
        writeln!(prefixes_file, "  ignore: false").unwrap();
        writeln!(prefixes_file, "  group: replay").unwrap();
        writeln!(prefixes_file).unwrap();
    }
}