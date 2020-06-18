use spdk_sys::{
    spdk_cpuset,
    spdk_cpuset_set_cpu,
    spdk_cpuset_zero,
    spdk_env_get_core_count,
    spdk_env_get_current_core,
    spdk_env_get_first_core,
    spdk_env_get_last_core,
    spdk_env_get_next_core,
};

#[derive(Debug)]
pub enum Core {
    Count,
    Current,
    First,
    Last,
}

#[derive(Debug)]
pub struct Cores(u32);

impl Cores {
    pub fn count() -> Self {
        Cores(Self::get_core(Core::Count))
    }

    pub fn first() -> u32 {
        Self::get_core(Core::First)
    }

    pub fn last() -> Self {
        Cores(Self::get_core(Core::Last))
    }

    pub fn current() -> u32 {
        unsafe { spdk_env_get_current_core() }
    }

    fn get_core(c: Core) -> u32 {
        unsafe {
            match c {
                Core::Count => spdk_env_get_core_count(),
                Core::Current => spdk_env_get_current_core(),
                Core::First => spdk_env_get_first_core(),
                Core::Last => spdk_env_get_last_core(),
            }
        }
    }
}

impl IntoIterator for Cores {
    type Item = u32;
    type IntoIter = CoreInterator;

    fn into_iter(self) -> Self::IntoIter {
        CoreInterator {
            current: std::u32::MAX,
        }
    }
}

pub struct CoreInterator {
    current: u32,
}

impl Iterator for CoreInterator {
    type Item = u32;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current == std::u32::MAX {
            self.current = Cores::get_core(Core::First);
            return Some(self.current);
        }

        self.current = unsafe { spdk_env_get_next_core(self.current) };

        if self.current == std::u32::MAX {
            None
        } else {
            Some(self.current)
        }
    }
}

pub struct CpuMask(spdk_cpuset);

impl CpuMask {
    pub fn new() -> Self {
        let mut mask = spdk_cpuset::default();
        unsafe { spdk_cpuset_zero(&mut mask) }
        Self(mask)
    }

    pub fn set_cpu(&mut self, cpu: u32, state: bool) {
        unsafe {
            spdk_cpuset_set_cpu(&mut self.0, cpu, state);
        }
    }
    pub fn as_ptr(&self) -> *mut spdk_cpuset {
        &self.0 as *const _ as *mut spdk_cpuset
    }
}
