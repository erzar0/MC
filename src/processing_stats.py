from typing import List, Dict, Any

class ProcessingStats:
    """
    Mutable statistics tracker for the Minecraft World Data Preparation Pipeline.
    Accumulates data about world conversion, extraction, and captioning.
    Prints a nicely formatted summary table at the end.
    """
    def __init__(self):
        self.processed_worlds: List[Dict[str, Any]] = []

    def record_world(
        self,
        world_name: str,
        status: str,
        error: str = "",
        world_size_mb: float = 0.0,
        volumes_count: int = 0,
        screenshots_count: int = 0,
        captions_total: int = 0,
        captions_generated: int = 0,
        captions_skipped: int = 0,
        captions_failed: int = 0
    ):
        """
        Record the processing outcome of a single Minecraft world.
        """
        self.processed_worlds.append({
            "world_name": world_name,
            "status": status,
            "error": error,
            "world_size_mb": world_size_mb,
            "volumes_count": volumes_count,
            "screenshots_count": screenshots_count,
            "captions_total": captions_total,
            "captions_generated": captions_generated,
            "captions_skipped": captions_skipped,
            "captions_failed": captions_failed
        })

    def print_summary(self):
        """
        Print a formatted table summary of the processed worlds to the console.
        """
        if not self.processed_worlds:
            print("\n*** No worlds were processed. ***\n")
            return

        print("\n" + "=" * 120)
        print(f"{'WORLD PROCESSING SUMMARY':^120}")
        print("=" * 120)

        # Header
        print(f"{'World Name':<28} | {'Status':<8} | {'Size(MB)':<8} | {'Vols':<4} | {'Scrns':<5} | {'Caps(G/S/F)':<13} | {'Error Details'}")
        print("-" * 120)

        success_count = 0
        failed_count = 0
        total_captions = 0
        total_volumes = 0
        total_screenshots = 0
        total_size_mb = 0.0
        
        for w in self.processed_worlds:
            # Truncating Long names
            raw_name = w['world_name']
            name_str = (raw_name[:25] + "...") if len(raw_name) > 28 else raw_name
            
            status_str = w['status']
            if status_str.lower() == "success":
                success_count += 1
            else:
                failed_count += 1
                
            vols = w.get('volumes_count', 0)
            scrns = w.get('screenshots_count', 0)
            size_mb = w.get('world_size_mb', 0.0)
            
            total_volumes += vols
            total_screenshots += scrns
            total_size_mb += size_mb
            
            size_str = f"{size_mb:.1f}"
            
            # Caption format
            gen, skip, fail = w['captions_generated'], w['captions_skipped'], w['captions_failed']
            caps_str = f"{gen:^3}/{skip:^3}/{fail:^3}"
            total_captions += gen
            
            # Error format
            err_str = str(w['error'])
            if err_str:
                # Optionally truncate error message if it's exceedingly long
                err_str = (err_str[:27] + "...") if len(err_str) > 30 else err_str
                
            print(f"{name_str:<28} | {status_str:<8} | {size_str:<8} | {vols:<4} | {scrns:<5} | {caps_str:<13} | {err_str}")

        print("=" * 120)
        # Footer totals
        total_worlds = len(self.processed_worlds)
        success_percentage = (success_count / total_worlds * 100) if total_worlds > 0 else 0.0
        
        print(f"Total Worlds Processed: {total_worlds}")
        print(f"Successful: {success_count} ({success_percentage:.1f}%)")
        print(f"Failed:     {failed_count}")
        print(f"Total Original Size:      {total_size_mb:.1f} MB")
        print(f"Total Volumes (Tensors):  {total_volumes}")
        print(f"Total Screenshots:        {total_screenshots}")
        print(f"Total Captions Generated: {total_captions}")
        print("=" * 120 + "\n")
