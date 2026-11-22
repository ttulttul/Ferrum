use serde_json::{json, Value};

#[derive(Clone, Copy, PartialEq)]
pub(super) struct Size {
    pub width: u64,
    pub height: u64,
}

impl Size {
    pub const DEFAULT: Self = Self {
        width: 1280,
        height: 800,
    };
    pub fn value(self) -> Value {
        json!({"width": self.width, "height": self.height})
    }
    pub fn event(self) -> String {
        json!({"type":"viewport", "width": self.width, "height": self.height}).to_string()
    }
    pub fn layout(self, zoom: f64) -> Self {
        let scale = |n: u64| (n as f64 / zoom).round().max(1.0) as u64;
        Self {
            width: scale(self.width),
            height: scale(self.height),
        }
    }
    pub fn fixed(setting: &Value) -> Option<Self> {
        let dimension = |v: &Value| {
            let n = v.as_f64()?;
            (n.fract() == 0.0 && (240.0..=3840.0).contains(&n)).then_some(n as u64)
        };
        let width = dimension(&setting["width"])?;
        let height = dimension(&setting["height"])?;
        (width * height <= 3840 * 2160).then_some(Self { width, height })
    }
    pub fn requested(message: &Value) -> Option<Self> {
        let width = message["width"].as_f64()?.round().min(3840.0) as u64;
        let height = message["height"].as_f64()?.round().min(2160.0) as u64;
        (width >= 100 && height >= 100).then_some(Self { width, height })
    }
}

// Chromium device sizes from the pinned reference's shared/previewViewport.
fn preset(id: &str) -> Option<(u64, u64)> {
    Some(match id {
        "iphone-se" => (375, 667),
        "iphone-xr" => (414, 896),
        "iphone-12-pro" => (390, 844),
        "iphone-14-pro-max" => (430, 932),
        "pixel-7" => (412, 915),
        "samsung-galaxy-s8-plus" => (360, 740),
        "samsung-galaxy-s20-ultra" => (412, 915),
        "ipad-mini" => (768, 1024),
        "ipad-air" => (820, 1180),
        "ipad-pro" => (1024, 1366),
        "surface-pro-7" => (912, 1368),
        "surface-duo" => (540, 720),
        "galaxy-z-fold-5" => (344, 882),
        "asus-zenbook-fold" => (853, 1280),
        "samsung-galaxy-a51-71" => (412, 914),
        "nest-hub" => (1024, 600),
        "nest-hub-max" => (1280, 800),
        _ => return None,
    })
}

pub(super) fn resolve(args: &Value) -> Option<Value> {
    match args["mode"].as_str()? {
        "fill" => Some(json!({"_tag":"fill"})),
        "freeform" => {
            let size = Size::fixed(args)?;
            Some(json!({"_tag":"freeform","width":size.width,"height":size.height}))
        }
        "preset" => {
            let id = args["preset"].as_str()?;
            let (mut width, mut height) = preset(id)?;
            match args["orientation"].as_str() {
                Some("landscape") if height > width => std::mem::swap(&mut width, &mut height),
                Some("portrait") if width > height => std::mem::swap(&mut width, &mut height),
                _ => {}
            }
            Some(json!({"_tag":"preset","width":width,"height":height,"presetId":id}))
        }
        _ => None,
    }
}

pub(super) fn viewer_setting(setting: &Value) -> Option<Value> {
    match setting["_tag"].as_str()? {
        "fill" => Some(json!({"_tag":"fill"})),
        "freeform" => {
            let size = Size::fixed(setting)?;
            Some(json!({"_tag":"freeform","width":size.width,"height":size.height}))
        }
        "preset" => {
            let size = Size::fixed(setting)?;
            let id = setting["presetId"].as_str()?;
            let legacy = matches!(
                id,
                "desktop-1920x1080"
                    | "desktop-1440x900"
                    | "laptop-1366x768"
                    | "laptop-1280x800"
                    | "ipad-pro-11"
                    | "iphone-15-pro"
                    | "pixel-8"
                    | "galaxy-s24"
            );
            (legacy || preset(id).is_some()).then(
                || json!({"_tag":"preset","width":size.width,"height":size.height,"presetId":id}),
            )
        }
        _ => None,
    }
}
