//! `platform.machine` in the environment descriptor: what kind of computer
//! the server runs on, detected the way the reference's
//! `ServerEnvironmentMachine.ts` does. Linux reads DMI (a VM or cloud vendor
//! reads as `cloud` whatever its chassis), with WSL reported as `linux`;
//! macOS reads the Apple product name; anything else, or a host the probes
//! cannot classify, has no machine kind and the field is left out.
use std::{path::Path, sync::OnceLock, time::Duration};

const DMI_ROOT: &str = "/sys/class/dmi/id";
const KERNEL_RELEASE_PATH: &str = "/proc/sys/kernel/osrelease";

/// SMBIOS 3.x System Enclosure types (table 17).
fn chassis_kind(chassis_type: &str) -> Option<&'static str> {
    Some(match chassis_type {
        "3" | "4" | "5" | "6" | "7" | "13" | "15" | "16" | "35" => "desktop",
        "8" | "9" | "10" | "14" | "31" | "32" => "laptop",
        "17" | "18" | "19" | "20" | "21" | "22" | "23" | "24" | "28" => "server",
        _ => return None,
    })
}

/// VMs read as cloud regardless of chassis. Hyper-V matches on its product
/// ("virtual machine"), since Surface shares its vendor.
const VIRTUALIZATION_MARKERS: &[&str] = &[
    "qemu",
    "kvm",
    "bochs",
    "vmware",
    "virtualbox",
    "innotek",
    "xen",
    "parallels",
    "amazon ec2",
    "google compute engine",
    "digitalocean",
    "hetzner",
    "linode",
    "vultr",
    "scaleway",
    "openstack",
    "cloud",
    "virtual machine",
];

/// Trimmed, or `None` when blank.
fn normalize(value: Option<&str>) -> Option<&str> {
    value.map(str::trim).filter(|v| !v.is_empty())
}

/// Classifies an Apple marketing name or `hw.model` ("Mac mini", "MacBookPro18,3").
pub fn from_apple_product_name(name: &str) -> Option<&'static str> {
    let normalized: String = name
        .trim()
        .to_lowercase()
        .chars()
        .filter(|c| !c.is_whitespace())
        .collect();
    if normalized.starts_with("macmini") {
        Some("mac-mini")
    } else if normalized.starts_with("macstudio") {
        Some("mac-studio")
    } else if normalized.starts_with("macbook") {
        Some("laptop")
    } else if normalized.starts_with("imac") || normalized.starts_with("macpro") {
        Some("desktop")
    } else {
        None
    }
}

/// Classifies DMI's `chassis_type`, `sys_vendor` and `product_name`.
pub fn from_dmi(
    chassis_type: Option<&str>,
    sys_vendor: Option<&str>,
    product_name: Option<&str>,
) -> Option<&'static str> {
    let product_name = normalize(product_name).unwrap_or("");
    let vendor_and_product =
        format!("{} {}", normalize(sys_vendor).unwrap_or(""), product_name).to_lowercase();
    if VIRTUALIZATION_MARKERS
        .iter()
        .any(|marker| vendor_and_product.contains(marker))
    {
        return Some("cloud");
    }
    if let Some(kind) = from_apple_product_name(product_name) {
        return Some(kind);
    }
    normalize(chassis_type).and_then(chassis_kind)
}

/// Linux, from the kernel release and DMI fields. WSL is checked first: WSL 2
/// presents as a Hyper-V VM in DMI.
pub fn linux(
    kernel_release: Option<&str>,
    chassis_type: Option<&str>,
    sys_vendor: Option<&str>,
    product_name: Option<&str>,
) -> Option<&'static str> {
    if normalize(kernel_release).is_some_and(|r| r.to_lowercase().contains("microsoft")) {
        return Some("linux");
    }
    from_dmi(chassis_type, sys_vendor, product_name)
}

/// macOS, from `ioreg -rd1 -n product` output, falling back to `hw.model`.
/// Apple silicon exposes IOKit's marketing name; Intel needs the fallback.
pub fn darwin(
    ioreg: Option<&str>,
    hw_model: impl FnOnce() -> Option<String>,
) -> Option<&'static str> {
    if let Some(kind) = normalize(ioreg)
        .and_then(ioreg_product_name)
        .and_then(from_apple_product_name)
    {
        return Some(kind);
    }
    hw_model()
        .as_deref()
        .and_then(|m| normalize(Some(m)))
        .and_then(from_apple_product_name)
}

/// The value of `"product-name" = <"...">` in ioreg output.
fn ioreg_product_name(ioreg: &str) -> Option<&str> {
    let at = ioreg.find("\"product-name\"")?;
    let rest = ioreg[at + "\"product-name\"".len()..].trim_start();
    let rest = rest.strip_prefix('=')?.trim_start();
    let rest = rest.strip_prefix("<\"")?;
    let end = rest.find('"')?;
    Some(&rest[..end]).filter(|s| !s.is_empty())
}

fn read_optional(path: &Path) -> Option<String> {
    let text = std::fs::read_to_string(path).ok()?;
    normalize(Some(&text)).map(str::to_owned)
}

/// Runs a probe for at most five seconds; its trimmed stdout on exit code 0.
fn run_probe(command: &str, args: &[&str]) -> Option<String> {
    let mut child = crate::login_shell::command(command)
        .args(args)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .spawn()
        .ok()?;
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        match child.try_wait() {
            Ok(Some(status)) => {
                if !status.success() {
                    return None;
                }
                let mut out = String::new();
                use std::io::Read;
                child.stdout.take()?.read_to_string(&mut out).ok()?;
                return normalize(Some(&out)).map(str::to_owned);
            }
            Ok(None) if std::time::Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(20));
            }
            _ => {
                let _ = child.kill();
                let _ = child.wait();
                return None;
            }
        }
    }
}

fn detect() -> Option<&'static str> {
    match std::env::consts::OS {
        "linux" => {
            let dmi = |name: &str| read_optional(&Path::new(DMI_ROOT).join(name));
            linux(
                read_optional(Path::new(KERNEL_RELEASE_PATH)).as_deref(),
                dmi("chassis_type").as_deref(),
                dmi("sys_vendor").as_deref(),
                dmi("product_name").as_deref(),
            )
        }
        "macos" => darwin(
            run_probe("ioreg", &["-rd1", "-n", "product"]).as_deref(),
            || run_probe("sysctl", &["-n", "hw.model"]),
        ),
        _ => None,
    }
}

/// This host's machine kind, detected once per process.
pub fn current() -> Option<&'static str> {
    static KIND: OnceLock<Option<&'static str>> = OnceLock::new();
    *KIND.get_or_init(detect)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cloud_vms_read_as_cloud_whatever_the_chassis() {
        // A GitHub Actions runner (EC2, chassis "Other").
        assert_eq!(
            from_dmi(Some("1"), Some("Amazon EC2"), Some("m5.large")),
            Some("cloud")
        );
        // Nitro reports a desktop-ish chassis; the vendor still wins.
        assert_eq!(
            from_dmi(Some("3"), Some("Amazon EC2"), Some("t3.micro")),
            Some("cloud")
        );
        assert_eq!(
            from_dmi(
                Some("1"),
                Some("QEMU"),
                Some("Standard PC (Q35 + ICH9, 2009)")
            ),
            Some("cloud")
        );
        assert_eq!(
            from_dmi(Some("1"), Some("Google"), Some("Google Compute Engine")),
            Some("cloud")
        );
        assert_eq!(
            from_dmi(Some("1"), Some("innotek GmbH"), Some("VirtualBox")),
            Some("cloud")
        );
        assert_eq!(
            from_dmi(Some("1"), Some("VMware, Inc."), Some("VMware7,1")),
            Some("cloud")
        );
        assert_eq!(
            from_dmi(Some("1"), Some("DigitalOcean"), Some("Droplet")),
            Some("cloud")
        );
        assert_eq!(
            from_dmi(Some("17"), Some("Hetzner"), Some("vServer")),
            Some("cloud")
        );
        assert_eq!(
            from_dmi(Some("1"), Some("Xen"), Some("HVM domU")),
            Some("cloud")
        );
        // Hyper-V matches on its product; Surface shares the vendor.
        assert_eq!(
            from_dmi(
                Some("3"),
                Some("Microsoft Corporation"),
                Some("Virtual Machine")
            ),
            Some("cloud")
        );
        assert_eq!(
            from_dmi(
                Some("9"),
                Some("Microsoft Corporation"),
                Some("Surface Laptop 5")
            ),
            Some("laptop")
        );
    }

    #[test]
    fn physical_hosts_read_from_the_chassis_type() {
        // A physical desktop (MSI board, chassis "Desktop").
        assert_eq!(
            from_dmi(
                Some("3"),
                Some("Micro-Star International Co., Ltd."),
                Some("MS-7D69")
            ),
            Some("desktop")
        );
        assert_eq!(
            from_dmi(Some("10"), Some("LENOVO"), Some("21CB")),
            Some("laptop")
        );
        assert_eq!(
            from_dmi(Some("23"), Some("Dell Inc."), Some("PowerEdge R740")),
            Some("server")
        );
        assert_eq!(
            from_dmi(Some("35"), Some("ASUS"), Some("PN50")),
            Some("desktop")
        );
        assert_eq!(
            from_dmi(Some(" 31\n"), Some("HP"), Some("Spectre")),
            Some("laptop")
        );
        // Unknown or missing chassis: no machine kind.
        assert_eq!(from_dmi(Some("1"), Some("Supermicro"), Some("X11")), None);
        assert_eq!(from_dmi(Some("2"), None, None), None);
        assert_eq!(from_dmi(None, None, None), None);
        assert_eq!(from_dmi(Some(""), Some(""), Some("")), None);
    }

    #[test]
    fn apple_hardware_reads_from_the_product_name() {
        assert_eq!(
            from_dmi(Some("10"), Some("Apple Inc."), Some("Mac mini")),
            Some("mac-mini")
        );
        assert_eq!(from_apple_product_name("Mac Studio"), Some("mac-studio"));
        assert_eq!(from_apple_product_name("MacBookPro18,3"), Some("laptop"));
        assert_eq!(from_apple_product_name("iMac21,1"), Some("desktop"));
        assert_eq!(from_apple_product_name("MacPro7,1"), Some("desktop"));
        assert_eq!(from_apple_product_name("Macmini9,1"), Some("mac-mini"));
        assert_eq!(from_apple_product_name("VirtualMac2,1"), None);
    }

    #[test]
    fn wsl_reads_as_linux_before_its_hyper_v_dmi() {
        assert_eq!(
            linux(
                Some("5.15.153.1-microsoft-standard-WSL2"),
                Some("3"),
                Some("Microsoft Corporation"),
                Some("Virtual Machine")
            ),
            Some("linux")
        );
        assert_eq!(
            linux(
                Some("6.8.0-138-generic"),
                Some("3"),
                Some("Micro-Star International Co., Ltd."),
                Some("MS-7D69")
            ),
            Some("desktop")
        );
        assert_eq!(
            linux(
                Some("6.8.0-1017-aws"),
                Some("1"),
                Some("Amazon EC2"),
                Some("m5.large")
            ),
            Some("cloud")
        );
        // A container without /sys/class/dmi: no machine kind.
        assert_eq!(linux(Some("6.8.0-1017-aws"), None, None, None), None);
        assert_eq!(linux(None, None, None, None), None);
    }

    #[test]
    fn darwin_reads_ioreg_then_falls_back_to_hw_model() {
        let ioreg = "+-o product  <class IOPlatformDevice>\n  {\n    \"product-name\" = <\"Mac Studio\">\n  }";
        assert_eq!(
            darwin(Some(ioreg), || panic!("no fallback needed")),
            Some("mac-studio")
        );
        assert_eq!(
            darwin(None, || Some("MacBookPro16,1".into())),
            Some("laptop")
        );
        assert_eq!(
            darwin(Some("no product here"), || Some("Macmini8,1".into())),
            Some("mac-mini")
        );
        assert_eq!(darwin(None, || None), None);
        assert_eq!(darwin(None, || Some("VirtualMac2,1".into())), None);
    }
}
