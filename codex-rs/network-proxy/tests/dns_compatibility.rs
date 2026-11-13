use hickory_resolver::config::NameServerConfig;
use hickory_resolver::config::ResolverConfig;
use hickory_resolver::proto::op::Message;
use hickory_resolver::proto::op::OpCode;
use hickory_resolver::proto::op::ResponseCode;
use hickory_resolver::proto::rr::RData;
use hickory_resolver::proto::rr::Record;
use hickory_resolver::proto::rr::RecordType;
use hickory_resolver::proto::rr::rdata::A;
use hickory_resolver::proto::rr::rdata::AAAA;
use hickory_resolver::proto::rr::rdata::TXT;
use pretty_assertions::assert_eq;
use rama_dns::DnsResolver;
use rama_dns::HickoryDns;
use rama_dns::hickory::resolver as hickory_resolver;
use rama_net::address::Domain;
use std::net::Ipv4Addr;
use std::net::Ipv6Addr;
use std::time::Duration;

#[test]
fn malformed_svcb_alpn_is_rejected_without_panicking() {
    for value in ["1 . alpn=\"h", "1 . alpn=\""] {
        assert!(RData::try_from_str(RecordType::SVCB, value).is_err());
    }
}

#[tokio::test]
async fn built_in_configurations_construct_without_queries() {
    let _ = HickoryDns::new_google();
    let _ = HickoryDns::new_cloudflare();
    let _ = HickoryDns::new_quad9();
    assert!(HickoryDns::builder().try_build().is_ok());
}

#[tokio::test]
async fn loopback_dns_preserves_addresses_txt_chunks_and_negative_answers() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let address = socket.local_addr().unwrap();
        let mut server = NameServerConfig::udp(address.ip());
        server.connections[0].port = address.port();
        let dns = HickoryDns::builder()
            .with_config(ResolverConfig::from_name_servers(vec![server]))
            .try_build()
            .unwrap();
        let responder = tokio::spawn(async move {
            let mut buffer = [0; 4096];
            for _ in 0..4 {
                let (size, peer) = socket.recv_from(&mut buffer).await.unwrap();
                let request = Message::from_vec(&buffer[..size]).unwrap();
                let query = &request.queries[0];
                let mut reply = Message::response(request.metadata.id, OpCode::Query);
                reply.metadata.authoritative = true;
                reply.add_query(query.clone());
                if query.name().to_string() == "missing.example.test." {
                    reply.metadata.response_code = ResponseCode::NXDomain;
                } else {
                    let data = match query.query_type() {
                        RecordType::A => RData::A(A(Ipv4Addr::new(192, 0, 2, 1))),
                        RecordType::AAAA => RData::AAAA(AAAA("2001:db8::1".parse().unwrap())),
                        RecordType::TXT => RData::TXT(TXT::from_bytes(vec![b"first", b"second"])),
                        other => panic!("unexpected query: {other:?}"),
                    };
                    reply.add_answer(Record::from_rdata(query.name().clone(), 60, data));
                }
                socket
                    .send_to(&reply.to_vec().unwrap(), peer)
                    .await
                    .unwrap();
            }
        });
        let domain = Domain::from_static("example.test");
        assert_eq!(
            dns.ipv4_lookup(domain.clone()).await.unwrap(),
            vec![Ipv4Addr::new(192, 0, 2, 1)]
        );
        assert_eq!(
            dns.ipv6_lookup(domain.clone()).await.unwrap(),
            vec!["2001:db8::1".parse::<Ipv6Addr>().unwrap()]
        );
        assert_eq!(
            dns.txt_lookup(domain).await.unwrap(),
            vec![b"first".to_vec(), b"second".to_vec()]
        );
        assert!(
            dns.ipv4_lookup(Domain::from_static("missing.example.test"))
                .await
                .is_err()
        );
        responder.await.unwrap();
    })
    .await
    .expect("loopback DNS fixture timed out");
}
